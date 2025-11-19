defmodule Indexer.Fetcher.Cosmos.DashboardAPIClient do
  @moduledoc """
  Client for interacting with the Shardeum EVM Dashboard Indexer API
  to fetch Cosmos transactions.
  """

  require Logger

  alias Explorer.Helper
  alias HTTPoison.Response

  @default_timeout 30_000

  @doc """
  Fetches recent Cosmos transactions from the dashboard indexer.

  ## Parameters
  - `limit` - Maximum number of transactions to fetch (default: 100, max: 100)
  - `last_timestamp` - Optional timestamp to fetch transactions after

  ## Returns
  - `{:ok, list()}`: List of transactions
  - `{:error, reason}`: Error tuple
  """
  @spec fetch_recent_transactions(pos_integer(), DateTime.t() | nil) ::
          {:ok, list(map())} | {:error, term()}
  def fetch_recent_transactions(limit \\ 100, last_timestamp \\ nil) do
    url = build_url("/api/v1/transactions/recent", %{limit: min(limit, 100)})

    with {:ok, data} <- do_get(url),
         {:ok, transactions} <- extract_transactions(data) do
      # Filter transactions after last_timestamp if provided
      filtered =
        if last_timestamp do
          Enum.filter(transactions, fn tx ->
            tx["timestamp"] && DateTime.compare(parse_timestamp(tx["timestamp"]), last_timestamp) == :gt
          end)
        else
          transactions
        end

      {:ok, filtered}
    end
  end

  @doc """
  Fetches a single transaction by hash from the dashboard indexer.

  ## Parameters
  - `hash` - Transaction hash (Cosmos or EVM hash)

  ## Returns
  - `{:ok, map()}`: Transaction data with messages
  - `{:error, reason}`: Error tuple
  """
  @spec fetch_transaction(binary()) :: {:ok, map()} | {:error, term()}
  def fetch_transaction(hash) when is_binary(hash) do
    url = build_url("/api/v1/transactions/#{hash}")

    with {:ok, data} <- do_get(url),
         {:ok, transaction} <- extract_transaction(data) do
      {:ok, transaction}
    end
  end

  @doc """
  Fetches transactions for a specific address.

  ## Parameters
  - `address` - The address to fetch transactions for
  - `page` - Page number (default: 1)
  - `limit` - Results per page (default: 20, max: 100)

  ## Returns
  - `{:ok, list()}`: List of transactions
  - `{:error, reason}`: Error tuple
  """
  @spec fetch_address_transactions(binary(), pos_integer(), pos_integer()) ::
          {:ok, list(map())} | {:error, term()}
  def fetch_address_transactions(address, page \\ 1, limit \\ 20) when is_binary(address) do
    url = build_url("/api/v1/transactions/address/#{address}", %{page: page, limit: min(limit, 100)})

    with {:ok, data} <- do_get(url),
         {:ok, transactions} <- extract_transactions(data) do
      {:ok, transactions}
    end
  end

  @doc """
  Fetches transactions for a specific block height.

  ## Parameters
  - `height` - Block height

  ## Returns
  - `{:ok, list()}`: List of transactions
  - `{:error, reason}`: Error tuple
  """
  @spec fetch_block_transactions(non_neg_integer()) :: {:ok, list(map())} | {:error, term()}
  def fetch_block_transactions(height) when is_integer(height) and height >= 0 do
    url = build_url("/api/v1/transactions/block/#{height}")

    with {:ok, data} <- do_get(url),
         {:ok, transactions} <- extract_transactions(data) do
      {:ok, transactions}
    end
  end

  @doc """
  Health check for the dashboard API.

  ## Returns
  - `:ok`: API is healthy
  - `{:error, reason}`: API is not healthy
  """
  @spec health_check() :: :ok | {:error, term()}
  def health_check do
    url = build_url("/api/v1/health")

    case do_get(url) do
      {:ok, _data} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # Private functions

  defp do_get(url) do
    headers = [
      {"Content-Type", "application/json"},
      {"Accept", "application/json"}
    ]

    Logger.debug("Fetching from Dashboard API: #{url}")

    case HTTPoison.get(url, headers, recv_timeout: @default_timeout, timeout: @default_timeout) do
      {:ok, %Response{body: body, status_code: 200}} ->
        case Helper.decode_json(body) do
          %{"success" => true, "data" => data} ->
            {:ok, data}

          %{"success" => false, "error" => error} ->
            Logger.warning("Dashboard API returned error: #{error}")
            {:error, {:api_error, error}}

          other ->
            Logger.warning("Unexpected Dashboard API response format: #{inspect(other)}")
            {:error, :invalid_response_format}
        end

      {:ok, %Response{body: _body, status_code: 404}} ->
        {:error, :not_found}

      {:ok, %Response{body: body, status_code: status_code}} when status_code >= 400 ->
        Logger.warning("Dashboard API HTTP error #{status_code}: #{body}")
        {:error, {:http_error, status_code}}

      {:error, %HTTPoison.Error{reason: reason}} ->
        Logger.error("Dashboard API request failed: #{inspect(reason)}")
        {:error, {:connection_error, reason}}
    end
  end

  defp build_url(path, params \\ %{}) do
    base_url = get_base_url()

    query_string =
      if params == %{} do
        ""
      else
        "?" <> URI.encode_query(params)
      end

    "#{base_url}#{path}#{query_string}"
  end

  defp get_base_url do
    config = Application.get_env(:indexer, __MODULE__, [])
    base_url = Keyword.get(config, :base_url, "http://localhost:3001")
    String.trim_trailing(base_url, "/")
  end

  defp extract_transactions(data) when is_list(data), do: {:ok, data}
  defp extract_transactions(_), do: {:error, :invalid_transactions_format}

  defp extract_transaction(data) when is_map(data), do: {:ok, data}
  defp extract_transaction(_), do: {:error, :invalid_transaction_format}

  defp parse_timestamp(timestamp) when is_binary(timestamp) do
    case DateTime.from_iso8601(timestamp) do
      {:ok, datetime, _offset} -> datetime
      {:error, _} -> DateTime.utc_now()
    end
  end

  defp parse_timestamp(_), do: DateTime.utc_now()
end
