defmodule Indexer.Fetcher.CosmosTransactionHistorical do
  @moduledoc """
  Background worker that fetches historical Cosmos transactions from the Shardeum EVM Dashboard Indexer
  and imports them into Blockscout.

  This fetcher runs as a one-time catchup process to backfill historical data that
  was not captured by the real-time CosmosTransaction fetcher.
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

  @default_batch_size 100
  @default_poll_interval :timer.seconds(2)
  @default_max_pages 10_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Check if the historical fetcher is enabled.
  """
  def enabled? do
    Application.get_env(:indexer, __MODULE__)[:enabled] == true
  end

  @doc """
  Get the current state of the historical sync.
  """
  def get_state do
    if Process.whereis(__MODULE__) do
      GenServer.call(__MODULE__, :get_state)
    else
      {:error, :not_running}
    end
  end

  def init(opts) do
    if enabled?() do
      batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
      poll_interval = Keyword.get(opts, :poll_interval, @default_poll_interval)
      max_pages = Keyword.get(opts, :max_pages, @default_max_pages)

      # Check if dashboard API is available
      case DashboardAPIClient.health_check() do
        :ok ->
          Logger.info("Cosmos Historical Transaction Fetcher started")

          # Start from page 1 (most recent) and work backwards
          # We could also start from a specific page if we track progress
          current_page = get_last_synced_page() + 1

          schedule_next_fetch(poll_interval)

          {:ok,
           %{
             batch_size: batch_size,
             poll_interval: poll_interval,
             max_pages: max_pages,
             current_page: current_page,
             total_imported: 0,
             status: :running,
             last_error: nil
           }}

        {:error, reason} ->
          Logger.warning(
            "Dashboard API not available (#{inspect(reason)}), Historical fetcher will retry in #{poll_interval}ms"
          )

          schedule_next_fetch(poll_interval)

          {:ok,
           %{
             batch_size: batch_size,
             poll_interval: poll_interval,
             max_pages: max_pages,
             current_page: 1,
             total_imported: 0,
             status: :waiting_for_api,
             last_error: reason
           }}
      end
    else
      Logger.info("Cosmos Historical Transaction Fetcher is disabled")
      {:ok, %{status: :disabled}}
    end
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  def handle_info(:fetch, %{status: :disabled} = state) do
    {:noreply, state}
  end

  def handle_info(:fetch, %{status: :completed} = state) do
    {:noreply, state}
  end

  def handle_info(:fetch, state) do
    new_state =
      case fetch_and_import_page(state) do
        {:ok, imported_count, pagination} ->
          total = Map.get(pagination, "total", 0)
          page = state.current_page
          limit = state.batch_size

          # Check if we've reached the end of available data
          if imported_count == 0 or page * limit >= total do
            Logger.info("Historical Cosmos sync completed! Total imported: #{state.total_imported + imported_count}")

            %{
              state
              | status: :completed,
                total_imported: state.total_imported + imported_count,
                last_error: nil
            }
          else
            Logger.info(
              "Historical sync: page #{page}/#{ceil(total / limit)}, imported #{imported_count} transactions"
            )

            # Save progress
            save_last_synced_page(page)

            schedule_next_fetch(state.poll_interval)

            %{
              state
              | current_page: page + 1,
                total_imported: state.total_imported + imported_count,
                status: :running,
                last_error: nil
            }
          end

        {:error, reason} ->
          Logger.warning("Failed to fetch historical Cosmos transactions: #{inspect(reason)}")
          schedule_next_fetch(state.poll_interval * 2)

          %{state | status: :error, last_error: reason}
      end

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

  defp fetch_and_import_page(state) do
    Logger.debug("Fetching historical Cosmos transactions page #{state.current_page}...")

    with {:ok, transactions, pagination} <-
           DashboardAPIClient.fetch_transactions_paginated(state.current_page, state.batch_size),
         {:ok, imported_count} <- import_cosmos_transactions(transactions) do
      {:ok, imported_count, pagination}
    else
      {:error, reason} = error ->
        Logger.warning("Failed to fetch/import historical Cosmos transactions: #{inspect(reason)}")
        error

      error ->
        Logger.warning("Unexpected error in fetch_and_import_page: #{inspect(error)}")
        {:error, error}
    end
  end

  defp import_cosmos_transactions([]), do: {:ok, 0}

  defp import_cosmos_transactions(transactions) do
    Logger.debug("Importing #{length(transactions)} historical Cosmos transactions")

    # Transform dashboard transactions to Blockscout format
    {addresses, blockscout_transactions, balance_updates} =
      Enum.reduce(transactions, {[], [], []}, fn tx, {addrs, txs, balances} ->
        case transform_transaction(tx) do
          {:ok, transaction_params, address_params, balance_params} ->
            {addrs ++ address_params, [transaction_params | txs], balances ++ balance_params}

          {:error, reason} ->
            Logger.debug("Skipping transaction #{tx["hash"]}: #{inspect(reason)}")
            {addrs, txs, balances}
        end
      end)

    # Assign unique indices per block to avoid duplicate key constraint
    # Group by block_hash, then assign sequential indices
    blockscout_transactions = assign_unique_indices(blockscout_transactions)

    if length(blockscout_transactions) == 0 do
      {:ok, 0}
    else
      # Extract unique blocks from transactions
      blocks =
        blockscout_transactions
        |> Enum.map(fn tx ->
          # Check if EVM block already exists at this height
          evm_block_exists =
            case get_evm_block_at_height(tx.block_number) do
              {:ok, _} -> true
              :not_found -> false
            end

          if evm_block_exists do
            nil
          else
            # Extract Cosmos block timestamp from cosmos_data
            cosmos_timestamp =
              case tx.cosmos_data do
                %{timestamp: ts} when is_binary(ts) ->
                  case DateTime.from_iso8601(ts) do
                    {:ok, dt, _} -> dt
                    _ -> tx.inserted_at
                  end

                _ ->
                  tx.inserted_at
              end

            %{
              number: tx.block_number,
              hash: tx.block_hash,
              timestamp: cosmos_timestamp,
              consensus: true,
              gas_used: tx.cumulative_gas_used || 0,
              gas_limit: (tx.cumulative_gas_used || 0) + 1_000_000,
              parent_hash: create_synthetic_block_hash(max(0, tx.block_number - 1)),
              miner_hash: tx.from_address_hash,
              nonce: tx.block_number,
              size: 0,
              difficulty: 0,
              total_difficulty: 0
            }
          end
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq_by(& &1.number)

      # Import blocks with on_conflict: :nothing to avoid overwriting EVM blocks
      block_import_result =
        if length(blocks) > 0 do
          Chain.import(%{blocks: %{params: blocks, on_conflict: :nothing}})
        else
          {:ok, %{}}
        end

      with {:ok, _block_result} <- block_import_result,
           {:ok, _addr_result} <- Chain.import(%{addresses: %{params: addresses}}),
           {:ok, tx_result} <- import_transactions(blockscout_transactions) do
        # Update balances asynchronously
        update_balances(balance_updates)

        {:ok, length(blockscout_transactions)}
      else
        {:error, step, failed_value, _changes_so_far} ->
          Logger.error("Import failed at step #{step}: #{inspect(failed_value)}")
          {:error, {step, failed_value}}

        error ->
          Logger.error("Import unexpected error: #{inspect(error)}")
          {:error, error}
      end
    end
  end

  defp transform_transaction(tx) do
    try do
      # Parse hashes
      {:ok, hash} = parse_hash(tx["hash"])
      alt_hash = if tx["evmHash"], do: parse_hash(tx["evmHash"]) |> elem(1), else: nil

      # Skip if an EVM transaction with the same hash already exists
      if alt_hash && evm_transaction_exists?(alt_hash) do
        {:error, "EVM transaction already exists with hash #{alt_hash}"}
      else
        # Skip if this Cosmos transaction already exists
        if cosmos_transaction_exists?(hash) do
          {:error, "Cosmos transaction already exists"}
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
      end
    rescue
      e ->
        Logger.error("Failed to transform transaction: #{inspect(e)}")
        {:error, e}
    end
  end

  defp cosmos_transaction_exists?(hash) do
    Repo.exists?(from(t in Transaction, where: t.hash == ^hash))
  end

  defp evm_transaction_exists?(hash) do
    Repo.exists?(from(t in Transaction, where: t.hash == ^hash))
  end

  defp process_transaction(tx, hash, alt_hash, from_hash, to_hash) do
    # Parse amounts
    amount_wei = parse_amount(tx["amount"])
    fee_wei = parse_amount(tx["fee"])

    # Parse gas values
    gas_wanted = parse_integer(tx["gasWanted"]) || 0
    gas_used = parse_integer(tx["gasUsed"]) || 0
    cosmos_height = parse_integer(tx["height"]) || 0

    # Check if EVM block already exists at this height - use its hash if so
    block_hash =
      case get_evm_block_at_height(cosmos_height) do
        {:ok, evm_hash} ->
          evm_hash

        :not_found ->
          create_synthetic_block_hash(cosmos_height)
      end

    # Determine status
    status = 1

    # Use from_address as placeholder if to_address is nil
    effective_to_hash = to_hash || from_hash

    # Build transaction params
    transaction_params = %{
      hash: hash,
      transaction_type: :cosmos,
      alt_hash: alt_hash,
      block_number: cosmos_height,
      block_hash: block_hash,
      from_address_hash: from_hash,
      to_address_hash: effective_to_hash,
      value: amount_wei,
      gas: gas_wanted,
      gas_used: gas_used,
      gas_price: if(gas_used > 0, do: Decimal.div(fee_wei, gas_used), else: 0),
      status: status,
      cosmos_data: %{
        memo: tx["memo"],
        denom: tx["denom"] || "ashm",
        category: tx["category"],
        type: tx["type"],
        fee_denom: tx["feeDenom"] || tx["denom"] || "ashm",
        fee_amount_raw: tx["feeAmount"],
        fee_amount_shm: tx["fee"],
        timestamp: tx["timestamp"],
        cosmos_height: cosmos_height,
        original_from_address: tx["fromAddress"] || tx["from"],
        original_to_address: tx["toAddress"] || tx["to"],
        gas_wanted: gas_wanted,
        gas_used: gas_used,
        data: tx["data"]
      },
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

    # Build address params
    address_params =
      [
        %{hash: from_hash},
        if(effective_to_hash != from_hash, do: %{hash: effective_to_hash}, else: nil)
      ]
      |> Enum.reject(&is_nil/1)

    # Build balance update params
    from_cosmos_addr = tx["fromAddress"] || tx["from"]
    to_cosmos_addr = tx["toAddress"] || tx["to"]

    balance_params =
      [
        %{address_hash: from_hash, cosmos_address: from_cosmos_addr},
        if(to_hash && to_hash != from_hash,
          do: %{address_hash: to_hash, cosmos_address: to_cosmos_addr},
          else: nil
        )
      ]
      |> Enum.reject(&is_nil/1)

    {:ok, transaction_params, address_params, balance_params}
  end

  defp import_transactions(transactions) do
    Chain.import(%{
      transactions: %{
        params: transactions,
        on_conflict: :nothing
      }
    })
  end

  # Assign unique transaction indices per block to avoid duplicate key constraint on (block_hash, index)
  # First checks existing indices in the DB and starts from the next available index
  defp assign_unique_indices(transactions) do
    # Group transactions by block_hash
    grouped = Enum.group_by(transactions, & &1.block_hash)

    # For each block, get existing max index and assign new indices starting from max+1
    Enum.flat_map(grouped, fn {block_hash, txs} ->
      # Get the current max index for this block from the database
      existing_max_index = get_max_transaction_index_for_block(block_hash)

      # Assign indices starting from existing_max + 1
      txs
      |> Enum.with_index(existing_max_index + 1)
      |> Enum.map(fn {tx, idx} ->
        %{tx | index: idx}
      end)
    end)
  end

  defp get_max_transaction_index_for_block(block_hash) do
    query =
      from(t in Transaction,
        where: t.block_hash == ^block_hash,
        select: max(t.index)
      )

    case Repo.one(query) do
      nil -> -1  # No transactions yet, start from 0
      max_idx -> max_idx
    end
  rescue
    _ -> -1
  end

  defp update_balances(balance_params) do
    # Deduplicate and schedule balance fetches
    balance_params
    |> Enum.uniq_by(fn %{address_hash: addr} -> addr end)
    |> Enum.each(fn params ->
      address_hash = params.address_hash
      latest_block_number = get_latest_evm_block_number()
      fetch_params = %{address_hash: address_hash, block_number: latest_block_number}
      CoinBalance.Catchup.async_fetch_balances([fetch_params])
    end)

    :ok
  end

  defp get_latest_evm_block_number do
    query =
      from(b in Block,
        where: b.consensus == true,
        order_by: [desc: b.number],
        limit: 1,
        select: b.number
      )

    Repo.one(query) || 0
  rescue
    _ -> 0
  end

  defp get_evm_block_at_height(block_number) do
    query =
      from(b in Block,
        where: b.consensus == true and b.number == ^block_number,
        select: b.hash,
        limit: 1
      )

    case Repo.one(query) do
      nil -> :not_found
      hash -> {:ok, hash}
    end
  rescue
    _ -> :not_found
  end

  defp parse_hash(hash_string) when is_binary(hash_string) do
    hash_bytes =
      if String.starts_with?(hash_string, "0x") do
        hash_string |> String.slice(2..-1//1) |> Base.decode16!(case: :mixed)
      else
        :crypto.hash(:sha256, hash_string)
      end

    Hash.cast(Hash.Full, hash_bytes)
  end

  defp parse_address(nil), do: nil
  defp parse_address(""), do: nil

  defp parse_address(address_string) when is_binary(address_string) do
    cond do
      String.starts_with?(address_string, "0x") ->
        case Hash.cast(Hash.Address, address_string) do
          {:ok, hash} -> hash
          _ -> nil
        end

      String.starts_with?(address_string, "shardeum") ->
        case decode_bech32_address(address_string) do
          {:ok, address_bytes} ->
            case Hash.cast(Hash.Address, address_bytes) do
              {:ok, hash} -> hash
              _ -> nil
            end

          _ ->
            nil
        end

      true ->
        nil
    end
  end

  defp decode_bech32_address(bech32_address) do
    try do
      case String.split(bech32_address, "1", parts: 2) do
        [_hrp, data_part] ->
          charset = ~c"qpzry9x8gf2tvdw0s3jn54khce6mua7l"
          data_chars = String.to_charlist(data_part)
          data_without_checksum = Enum.take(data_chars, length(data_chars) - 6)

          five_bit_values =
            Enum.map(data_without_checksum, fn char ->
              Enum.find_index(charset, &(&1 == char))
            end)

          address_bytes = convert_bits(five_bit_values, 5, 8, false)
          {:ok, :binary.list_to_bin(address_bytes)}

        _ ->
          {:error, :invalid_format}
      end
    rescue
      _ -> {:error, :decode_failed}
    end
  end

  defp convert_bits(data, from_bits, to_bits, pad) do
    acc = 0
    bits = 0
    max_v = (1 <<< to_bits) - 1

    {result, acc, bits} =
      Enum.reduce(data, {[], acc, bits}, fn value, {result, acc, bits} ->
        acc = (acc <<< from_bits) ||| value
        bits = bits + from_bits
        {new_result, new_acc, new_bits} = extract_bits(result, acc, bits, to_bits, max_v)
        {new_result, new_acc, new_bits}
      end)

    result =
      if pad and bits > 0 do
        result ++ [((acc <<< (to_bits - bits)) &&& max_v)]
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

  # Progress tracking using DETS (disk-based ETS)
  defp get_last_synced_page do
    dets_file = get_dets_file()

    case :dets.open_file(:cosmos_historical_sync, [{:file, dets_file}]) do
      {:ok, table} ->
        result =
          case :dets.lookup(table, :last_page) do
            [{:last_page, page}] -> page
            _ -> 0
          end

        :dets.close(table)
        result

      {:error, _} ->
        0
    end
  end

  defp save_last_synced_page(page) do
    dets_file = get_dets_file()

    case :dets.open_file(:cosmos_historical_sync, [{:file, dets_file}]) do
      {:ok, table} ->
        :dets.insert(table, {:last_page, page})
        :dets.close(table)

      {:error, reason} ->
        Logger.warning("Failed to save sync progress: #{inspect(reason)}")
    end
  end

  defp get_dets_file do
    dets_dir = Application.get_env(:indexer, :dets_dir, "./dets")
    File.mkdir_p!(dets_dir)
    Path.join(dets_dir, "cosmos_historical_sync.dets") |> String.to_charlist()
  end
end
