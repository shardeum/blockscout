defmodule Indexer.Fetcher.CosmosTransaction do
  @moduledoc """
  Periodically fetches Cosmos transactions from the Shardeum EVM Dashboard Indexer
  and imports them into Blockscout.
  """

  use GenServer

  require Logger

  import Ecto.Query
  import Bitwise

  alias Explorer.Chain
  alias Explorer.Chain.{Address, Block, Hash, Transaction}
  alias Explorer.Chain.Events.Publisher
  alias Explorer.Repo
  alias Indexer.Fetcher.Cosmos.DashboardAPIClient
  alias Indexer.Fetcher.CoinBalance

  @default_poll_interval :timer.seconds(5)
  @default_batch_size 100

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    poll_interval = Keyword.get(opts, :poll_interval, @default_poll_interval)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    # Check if dashboard API is available
    case DashboardAPIClient.health_check() do
      :ok ->
        Logger.info("Cosmos Transaction Fetcher started, polling every #{poll_interval}ms")
        schedule_next_fetch(poll_interval)

        {:ok,
         %{
           poll_interval: poll_interval,
           batch_size: batch_size,
           last_timestamp: get_latest_cosmos_timestamp()
         }}

      {:error, reason} ->
        Logger.warning(
          "Dashboard API not available (#{inspect(reason)}), Cosmos fetcher will retry in #{poll_interval}ms"
        )

        schedule_next_fetch(poll_interval)

        {:ok,
         %{
           poll_interval: poll_interval,
           batch_size: batch_size,
           last_timestamp: nil
         }}
    end
  end

  def handle_info(:fetch, state) do
    new_state =
      case fetch_and_import_transactions(state) do
        {:ok, new_timestamp} ->
          %{state | last_timestamp: new_timestamp || state.last_timestamp}

        {:error, reason} ->
          Logger.warning("Failed to fetch Cosmos transactions: #{inspect(reason)}")
          state
      end

    schedule_next_fetch(state.poll_interval)
    {:noreply, new_state}
  end

  # Catch-all for unexpected messages
  def handle_info(msg, state) do
    Logger.debug("Received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp schedule_next_fetch(interval) do
    Process.send_after(self(), :fetch, interval)
  end

  defp fetch_and_import_transactions(state) do
    Logger.debug("Fetching Cosmos transactions from dashboard API...")

    with {:ok, transactions} <-
           DashboardAPIClient.fetch_recent_transactions(state.batch_size, state.last_timestamp),
         {:ok, imported_count} <- import_cosmos_transactions(transactions) do
      Logger.info("Imported #{imported_count} Cosmos transactions")

      new_timestamp =
        transactions
        |> Enum.map(fn tx -> parse_timestamp(tx["timestamp"]) end)
        |> Enum.max(DateTime, fn -> state.last_timestamp end)

      {:ok, new_timestamp}
    else
      {:error, reason} = error ->
        Logger.warning("Failed to fetch/import Cosmos transactions: #{inspect(reason)}")
        error

      error ->
        Logger.warning("Unexpected error in fetch_and_import_transactions: #{inspect(error)}")
        {:error, error}
    end
  end

  defp import_cosmos_transactions([]), do: {:ok, 0}

  defp import_cosmos_transactions(transactions) do
    Logger.info("RAW: Importing #{length(transactions)} Cosmos transactions from dashboard")
    Logger.info("RAW: First raw tx: #{inspect(Enum.at(transactions, 0))}")

    # Transform dashboard transactions to Blockscout format
    {addresses, blockscout_transactions, balance_updates} =
      Enum.reduce(transactions, {[], [], []}, fn tx, {addrs, txs, balances} ->
        case transform_transaction(tx) do
          {:ok, transaction_params, address_params, balance_params} ->
            {addrs ++ address_params, [transaction_params | txs], balances ++ balance_params}

          {:error, reason} ->
            Logger.warning("Failed to transform transaction #{tx["hash"]}: #{inspect(reason)}")
            {addrs, txs, balances}
        end
      end)

    # Extract unique blocks from transactions with real Cosmos data
    blocks =
      blockscout_transactions
      |> Enum.map(fn tx ->
        # Extract Cosmos block timestamp from cosmos_data
        cosmos_timestamp =
          case tx.cosmos_data do
            %{timestamp: ts} when is_binary(ts) ->
              case DateTime.from_iso8601(ts) do
                {:ok, dt, _} -> dt
                _ -> tx.inserted_at
              end
            _ -> tx.inserted_at
          end

        %{
          number: tx.block_number,
          hash: tx.block_hash,
          timestamp: cosmos_timestamp,
          consensus: true,
          gas_used: tx.cumulative_gas_used || 0,
          gas_limit: (tx.cumulative_gas_used || 0) + 1000000,  # Add some headroom
          parent_hash: create_synthetic_block_hash(max(0, tx.block_number - 1)),
          miner_hash: tx.from_address_hash,
          nonce: tx.block_number,  # Use Cosmos block height as nonce
          size: 0,  # Cosmos doesn't track block size the same way
          difficulty: 0,  # Cosmos uses PoS, no difficulty
          total_difficulty: 0  # Cosmos uses PoS, no difficulty
        }
      end)
      |> Enum.uniq_by(& &1.number)

    Logger.info("Creating #{length(blocks)} Cosmos blocks, #{length(blockscout_transactions)} transactions")
    Logger.info("First block: #{inspect(Enum.at(blocks, 0))}")
    Logger.info("First tx: #{inspect(Enum.at(blockscout_transactions, 0))}")

    # Import blocks, addresses, transactions, and balances
    with {:ok, block_result} <- Chain.import(%{blocks: %{params: blocks}}) do
      Logger.info("Block import succeeded: #{inspect(block_result)}")

      case Chain.import(%{addresses: %{params: addresses}}) do
        {:ok, addr_result} ->
          Logger.info("Address import succeeded: #{inspect(addr_result)}")

          case import_transactions(blockscout_transactions) do
            {:ok, tx_result} ->
              Logger.info("Transaction import succeeded: #{inspect(tx_result)}")

              # Broadcast transactions event for real-time websocket updates
              imported_txs = Map.get(tx_result, :transactions, [])
              if length(imported_txs) > 0 do
                Publisher.broadcast([{:transactions, imported_txs}], :realtime)
              end

              case update_balances(balance_updates) do
                :ok ->
                  Logger.info("Balance update succeeded")
                  {:ok, length(blockscout_transactions)}
                error ->
                  Logger.error("Balance update failed: #{inspect(error)}")
                  error
              end
            error ->
              Logger.error("Transaction import failed: #{inspect(error)}")
              error
          end
        error ->
          Logger.error("Address import failed: #{inspect(error)}")
          error
      end
    else
      {:error, step, failed_value, _changes_so_far} ->
        Logger.error("Block import failed at step #{step}: #{inspect(failed_value)}")
        {:error, {step, failed_value}}

      error ->
        Logger.error("Block import unexpected error: #{inspect(error)}")
        {:error, error}
    end
  end

  defp transform_transaction(tx) do
    try do
      # Parse hashes
      {:ok, hash} = parse_hash(tx["hash"])
      alt_hash = if tx["evmHash"], do: parse_hash(tx["evmHash"]) |> elem(1), else: nil

      # Skip if an EVM transaction with the same hash already exists
      # This prevents duplicate display of native transfers that are indexed by both
      # the EVM indexer and the Cosmos fetcher
      if alt_hash && evm_transaction_exists?(alt_hash) do
        {:error, "EVM transaction already exists with hash #{alt_hash}"}
      else
        # Parse addresses
        from_hash = parse_address(tx["fromAddress"] || tx["from"])
        to_hash = parse_address(tx["toAddress"] || tx["to"])

        # Skip transactions without from_address (required by Blockscout)
        if is_nil(from_hash) do
          {:error, "from_address is required"}
        else
          process_transaction(tx, hash, alt_hash, from_hash, to_hash)
        end
      end
    rescue
      e ->
        Logger.error("Failed to transform transaction: #{inspect(e)}")
        {:error, e}
    end
  end

  # Check if an EVM transaction with the given hash already exists in the database
  defp evm_transaction_exists?(hash) do
    Repo.exists?(from t in Transaction, where: t.hash == ^hash)
  end

  defp process_transaction(tx, hash, alt_hash, from_hash, to_hash) do
    # Parse amounts
    # Dashboard API returns:
    # - "amount" as string (human readable, e.g., "0.111000")
    # - "feeAmount" as raw ashm integer (e.g., 287981000000000000000)
    # - "fee" as string (human readable, e.g., "0.287981")
    # Use the human readable versions and convert to Wei
    amount_wei = parse_amount(tx["amount"])
    # Use "fee" (human readable string) instead of "feeAmount" (raw ashm)
    fee_wei = parse_amount(tx["fee"])

      # Parse gas values
      gas_wanted = parse_integer(tx["gasWanted"]) || 0
      gas_used = parse_integer(tx["gasUsed"]) || 0

      # Parse Cosmos block number and add offset to avoid conflicts with EVM blocks
      # Use a high offset (1 billion) to ensure Cosmos blocks don't overlap with EVM blocks
      # This prevents Cosmos transactions from being overwritten by EVM blocks
      cosmos_height = parse_integer(tx["height"]) || 0
      cosmos_block_number = 1_000_000_000 + cosmos_height

      # Create synthetic block hash from Cosmos block number
      # This allows transactions to show as confirmed while maintaining referential integrity
      block_hash = create_synthetic_block_hash(cosmos_block_number)

      # Determine status - use 1 for success (Cosmos transactions from dashboard are confirmed)
      status = 1

      # For Cosmos transactions without a to_address (e.g., staking, governance),
      # use the from_address as a placeholder to prevent null constraint violations
      effective_to_hash = to_hash || from_hash

      # Build transaction params
      transaction_params = %{
        hash: hash,
        transaction_type: :cosmos,
        alt_hash: alt_hash,
        block_number: cosmos_block_number,
        block_hash: block_hash,
        from_address_hash: from_hash,
        to_address_hash: effective_to_hash,
        value: amount_wei,
        gas: gas_wanted,
        gas_used: gas_used,
        # Calculate gas_price using gas_used so that displayed fee (gas_price * gas_used) = actual fee
        # Blockscout displays fee as gas_price * gas_used, so we need to ensure this equals the actual fee paid
        gas_price: if(gas_used > 0, do: Decimal.div(fee_wei, gas_used), else: 0),
        status: status,
        # Cosmos-specific data stored as JSON
        cosmos_data: %{
          memo: tx["memo"],
          denom: tx["denom"] || "ashm",
          category: tx["category"],
          type: tx["type"],
          fee_denom: tx["feeDenom"] || tx["denom"] || "ashm",
          fee_amount_raw: tx["feeAmount"],  # Raw fee in ashm for display
          fee_amount_shm: tx["fee"],  # Human readable fee in SHM
          timestamp: tx["timestamp"],
          cosmos_height: cosmos_height,  # Original Cosmos block height
          original_from_address: tx["fromAddress"] || tx["from"],  # Original Cosmos from address
          original_to_address: tx["toAddress"] || tx["to"],  # Original Cosmos to address
          gas_wanted: gas_wanted,
          gas_used: gas_used,
          data: tx["data"]
        },
        # Set EVM fields - use dummy values for r,s,v since they're NOT NULL in DB
        input: nil,
        nonce: nil,
        r: 0,
        s: 0,
        v: 0,
        cumulative_gas_used: gas_used,
        index: 0,
        inserted_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      # Build address params - include both from and effective_to
      address_params =
        [
          %{hash: from_hash},
          if(effective_to_hash != from_hash, do: %{hash: effective_to_hash}, else: nil)
        ]
        |> Enum.reject(&is_nil/1)

      # Build balance update params
      # Include both EVM address hash and original Cosmos address for balance fetching
      from_cosmos_addr = tx["fromAddress"] || tx["from"]
      to_cosmos_addr = tx["toAddress"] || tx["to"]

      balance_params =
        [
          # Always include from_address for balance fetch
          %{address_hash: from_hash, cosmos_address: from_cosmos_addr},
          # Include to_address if it's different from from_address
          if(to_hash && to_hash != from_hash,
            do: %{address_hash: to_hash, cosmos_address: to_cosmos_addr},
            else: nil)
        ]
        |> Enum.reject(&is_nil/1)

      {:ok, transaction_params, address_params, balance_params}
  end

  defp import_transactions(transactions) do
    # Use Chain.import to insert transactions
    Chain.import(%{
      transactions: %{
        params: transactions,
        on_conflict: :replace_all
      }
    })
  end

  defp update_balances(balance_params) do
    # For Cosmos transactions, fetch balances from the Cosmos chain via Dashboard API
    # This ensures we get accurate balances for Cosmos addresses

    Logger.info("Fetching Cosmos address balances from dashboard API")

    # Deduplicate by address and fetch balances
    balance_params
    |> Enum.uniq_by(fn %{address_hash: addr} -> addr end)
    |> Enum.each(fn params ->
      cosmos_address = params[:cosmos_address]
      address_hash = params.address_hash

      if cosmos_address && String.starts_with?(cosmos_address, "shardeum") do
        # Fetch balance from Dashboard API for Cosmos addresses
        case DashboardAPIClient.fetch_account_balance(cosmos_address) do
          {:ok, balances} when is_list(balances) ->
            # Find ashm balance
            ashm_balance =
              Enum.find(balances, fn b -> b["denom"] == "ashm" end)
              |> case do
                nil -> 0
                %{"amount" => amount} when is_binary(amount) ->
                  case Integer.parse(amount) do
                    {val, _} -> val
                    :error -> 0
                  end
                _ -> 0
              end

            Logger.debug("Cosmos balance for #{cosmos_address}: #{ashm_balance} ashm")

            # Update the address balance directly in Blockscout
            update_address_balance(address_hash, ashm_balance)

          {:error, reason} ->
            Logger.warning("Failed to fetch Cosmos balance for #{cosmos_address}: #{inspect(reason)}")
            # Fall back to EVM balance fetcher
            fallback_evm_balance_fetch(address_hash)
        end
      else
        # For EVM addresses, use the standard balance fetcher
        fallback_evm_balance_fetch(address_hash)
      end
    end)

    :ok
  end

  defp update_address_balance(address_hash, balance_wei) when is_integer(balance_wei) do
    # Update the address record with the fetched balance
    case Repo.get_by(Address, hash: address_hash) do
      nil ->
        Logger.warning("Address not found for balance update: #{inspect(address_hash)}")

      address ->
        changeset = Address.balance_changeset(address, %{
          fetched_coin_balance: balance_wei,
          fetched_coin_balance_block_number: get_latest_evm_block_number()
        })

        case Repo.update(changeset) do
          {:ok, _updated} ->
            Logger.debug("Updated balance for #{inspect(address_hash)}: #{balance_wei}")

          {:error, error} ->
            Logger.error("Failed to update balance for #{inspect(address_hash)}: #{inspect(error)}")
        end
    end
  end

  defp fallback_evm_balance_fetch(address_hash) do
    latest_block_number = get_latest_evm_block_number()
    fetch_params = %{address_hash: address_hash, block_number: latest_block_number}
    CoinBalance.Catchup.async_fetch_balances([fetch_params])
  end

  defp get_latest_evm_block_number do
    # Query the latest REAL block number (not Cosmos synthetic blocks)
    # Cosmos blocks start at 1,000,000,000 so we exclude those
    query =
      from(b in Block,
        where: b.consensus == true and b.number < 1_000_000_000,
        order_by: [desc: b.number],
        limit: 1,
        select: b.number
      )

    case Repo.one(query) do
      nil ->
        # If no EVM blocks exist yet, use block 0 (genesis)
        # The balance fetcher will fetch at the current state
        Logger.warning("No EVM blocks found, using block 0 for balance fetch")
        0

      block_number ->
        Logger.debug("Latest EVM block number: #{block_number}")
        block_number
    end
  rescue
    error ->
      Logger.error("Error getting latest EVM block: #{inspect(error)}")
      0
  end

  defp parse_hash(hash_string) when is_binary(hash_string) do
    # Dashboard API returns hashes as strings (either hex or Cosmos bech32)
    # For Cosmos transactions, we'll create a deterministic hash from the string
    hash_bytes =
      if String.starts_with?(hash_string, "0x") do
        # EVM hash
        hash_string |> String.slice(2..-1//1) |> Base.decode16!(case: :mixed)
      else
        # Cosmos hash - use SHA256
        :crypto.hash(:sha256, hash_string)
      end

    Hash.cast(Hash.Full, hash_bytes)
  end

  defp parse_address(nil), do: nil
  defp parse_address(""), do: nil

  defp parse_address(address_string) when is_binary(address_string) do
    cond do
      # EVM address
      String.starts_with?(address_string, "0x") ->
        case Hash.cast(Hash.Address, address_string) do
          {:ok, hash} -> hash
          _ -> nil
        end

      # Shardeum Cosmos address (shardeum prefix) - decode bech32 to get EVM address
      # In Ethermint, Cosmos bech32 and EVM hex addresses share the same 20 bytes
      String.starts_with?(address_string, "shardeum") ->
        case decode_bech32_address(address_string) do
          {:ok, address_bytes} ->
            case Hash.cast(Hash.Address, address_bytes) do
              {:ok, hash} -> hash
              _ -> nil
            end
          _ -> nil
        end

      true ->
        nil
    end
  end

  # Decode a bech32 Cosmos address to its underlying 20-byte address
  # This is the same address used in EVM format
  defp decode_bech32_address(bech32_address) do
    try do
      # Split the address at "1" separator
      case String.split(bech32_address, "1", parts: 2) do
        [_hrp, data_part] ->
          # Decode the base32 data (bech32 charset)
          charset = ~c"qpzry9x8gf2tvdw0s3jn54khce6mua7l"

          # Remove the 6-character checksum from the end
          data_chars = String.to_charlist(data_part)
          data_without_checksum = Enum.take(data_chars, length(data_chars) - 6)

          # Convert from bech32 charset to 5-bit values
          five_bit_values = Enum.map(data_without_checksum, fn char ->
            Enum.find_index(charset, &(&1 == char))
          end)

          # Convert from 5-bit to 8-bit (the address bytes)
          address_bytes = convert_bits(five_bit_values, 5, 8, false)

          {:ok, :binary.list_to_bin(address_bytes)}
        _ ->
          {:error, :invalid_format}
      end
    rescue
      _ -> {:error, :decode_failed}
    end
  end

  # Convert between bit sizes (used for bech32 decoding)
  defp convert_bits(data, from_bits, to_bits, pad) do
    acc = 0
    bits = 0
    max_v = (1 <<< to_bits) - 1

    {result, acc, bits} = Enum.reduce(data, {[], acc, bits}, fn value, {result, acc, bits} ->
      acc = (acc <<< from_bits) ||| value
      bits = bits + from_bits

      {new_result, new_acc, new_bits} = extract_bits(result, acc, bits, to_bits, max_v)
      {new_result, new_acc, new_bits}
    end)

    result = if pad and bits > 0 do
      result ++ [(acc <<< (to_bits - bits)) &&& max_v]
    else
      result
    end

    result
  end

  defp extract_bits(result, acc, bits, to_bits, max_v) when bits >= to_bits do
    bits = bits - to_bits
    value = (acc >>> bits) &&& max_v
    extract_bits(result ++ [value], acc, bits, to_bits, max_v)
  end

  defp extract_bits(result, acc, bits, _to_bits, _max_v) do
    {result, acc, bits}
  end

  defp create_synthetic_block_hash(block_number) when is_integer(block_number) do
    # Create a deterministic block hash from the Cosmos block number
    # Format: "cosmos_block_<number>"
    block_string = "cosmos_block_#{block_number}"
    hash_bytes = :crypto.hash(:sha256, block_string)

    case Hash.cast(Hash.Full, hash_bytes) do
      {:ok, hash} -> hash
      _ -> nil
    end
  end

  defp create_synthetic_block_hash(_), do: nil

  defp parse_amount(nil), do: Decimal.new(0)

  defp parse_amount(amount) when is_binary(amount) do
    case Decimal.parse(amount) do
      {decimal, _} -> Decimal.mult(decimal, Decimal.new(1_000_000_000_000_000_000))
      :error -> Decimal.new(0)
    end
  end

  defp parse_amount(amount) when is_number(amount) do
    Decimal.mult(Decimal.from_float(amount * 1.0), Decimal.new(1_000_000_000_000_000_000))
  end

  defp parse_amount(_), do: Decimal.new(0)

  defp parse_integer(nil), do: nil

  defp parse_integer(val) when is_integer(val), do: val

  defp parse_integer(val) when is_binary(val) do
    case Integer.parse(val) do
      {int, _} -> int
      :error -> nil
    end
  end

  defp parse_integer(_), do: nil

  defp parse_timestamp(nil), do: nil

  defp parse_timestamp(timestamp) when is_binary(timestamp) do
    case DateTime.from_iso8601(timestamp) do
      {:ok, datetime, _offset} -> datetime
      {:error, _} -> nil
    end
  end

  defp parse_timestamp(_), do: nil

  defp get_latest_cosmos_timestamp do
    # Query the latest Cosmos transaction timestamp from the database
    query =
      from(t in Transaction,
        where: t.transaction_type == :cosmos,
        order_by: [desc: fragment("(cosmos_data->>'timestamp')::timestamp")],
        limit: 1,
        select: fragment("(cosmos_data->>'timestamp')::text")
      )

    case Repo.one(query) do
      nil -> nil
      timestamp_string -> parse_timestamp(timestamp_string)
    end
  rescue
    _error ->
      # If the column doesn't exist yet (before migration), return nil
      nil
  end
end
