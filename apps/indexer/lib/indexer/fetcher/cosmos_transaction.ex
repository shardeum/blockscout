defmodule Indexer.Fetcher.CosmosTransaction do
  @moduledoc """
  Periodically fetches Cosmos transactions from the Shardeum EVM Dashboard Indexer
  and imports them into Blockscout.
  """

  use GenServer

  require Logger

  import Ecto.Query

  alias Explorer.Chain
  alias Explorer.Chain.{Address, Block, Hash, Transaction}
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

      # Parse addresses
      from_hash = parse_address(tx["fromAddress"] || tx["from"])
      to_hash = parse_address(tx["toAddress"] || tx["to"])

      # Skip transactions without from_address (required by Blockscout)
      if is_nil(from_hash) do
        {:error, "from_address is required"}
      else
        process_transaction(tx, hash, alt_hash, from_hash, to_hash)
      end
    rescue
      e ->
        Logger.error("Failed to transform transaction: #{inspect(e)}")
        {:error, e}
    end
  end

  defp process_transaction(tx, hash, alt_hash, from_hash, to_hash) do
    # Parse amounts (convert from ashm to Wei - both use 18 decimals)
    amount_wei = parse_amount(tx["amount"])
    fee_wei = parse_amount(tx["feeAmount"] || tx["fee"])

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
        gas_price: if(gas_wanted > 0, do: Decimal.div(fee_wei, gas_wanted), else: 0),
        status: status,
        # Cosmos-specific data stored as JSON
        cosmos_data: %{
          memo: tx["memo"],
          denom: tx["denom"] || "ashm",
          category: tx["category"],
          type: tx["type"],
          fee_denom: tx["feeDenom"] || tx["denom"] || "ashm",
          timestamp: tx["timestamp"],
          cosmos_height: cosmos_height,  # Original Cosmos block height
          original_to_address: tx["toAddress"] || tx["to"],  # Store original to address
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
      # Important: Don't include block_number here, we'll add it later with the actual latest block
      # This is because we need to fetch balances from the EVM chain at the current block,
      # not at the synthetic Cosmos block number
      balance_params =
        [
          # Always include from_address for balance fetch
          %{address_hash: from_hash},
          # Include to_address if it's different from from_address
          if(to_hash && to_hash != from_hash, do: %{address_hash: to_hash}, else: nil)
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
    # For Cosmos transactions, we need to fetch the balance from the actual EVM/Cosmos chain
    # We'll use a high block number that's beyond any realistic block to force fetching "latest"
    # This is a workaround since the balance fetcher expects a block number

    # Get the maximum block number from the EVM chain (not Cosmos synthetic blocks)
    latest_block_number = get_latest_evm_block_number()

    Logger.info("Fetching Cosmos address balances at EVM block: #{latest_block_number}")

    # Deduplicate by address and fetch balances
    balance_params
    |> Enum.uniq_by(fn %{address_hash: addr} -> addr end)
    |> Enum.each(fn params ->
      # Use the latest EVM block number for balance fetching
      fetch_params = Map.put(params, :block_number, latest_block_number)

      Logger.debug("Queueing balance fetch for address #{inspect(fetch_params.address_hash)} at block #{fetch_params.block_number}")

      # Async fetch the actual balance from the chain
      CoinBalance.Catchup.async_fetch_balances([fetch_params])
    end)

    :ok
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

      # Shardeum Cosmos address (shardeum prefix) - create deterministic EVM address from it
      String.starts_with?(address_string, "shardeum") ->
        # Use first 20 bytes of SHA256 hash of the Cosmos address
        hash_bytes = :crypto.hash(:sha256, address_string) |> binary_part(0, 20)

        case Hash.cast(Hash.Address, hash_bytes) do
          {:ok, hash} -> hash
          _ -> nil
        end

      true ->
        nil
    end
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
