alias Explorer.Repo
alias Explorer.Chain.Address
require Logger

# Path comes from CHAIN_SPEC_PATH or fallback
chain_spec_path = System.get_env("CHAIN_SPEC_PATH", "/Users/jd-work/Work/Shardeum/Checkins/Blockscout/blockscout/blockscout-genesis.json")
Logger.info("Reading genesis allocs from #{chain_spec_path}")

# Read and decode JSON
{:ok, body} = File.read(chain_spec_path)
{:ok, json} = Jason.decode(body)

allocs =
  json
  |> get_in(["genesis", "alloc"])
  |> Kernel.||(%{})

Logger.info("Found #{map_size(allocs)} alloc entries")

Enum.each(allocs, fn {addr, data} ->
  balance_hex = Map.get(data, "balance", "0x0")
  balance =
    balance_hex
    |> String.trim_leading("0x")
    |> (fn h -> if h == "", do: 0, else: String.to_integer(h, 16) end).()

  # Insert or update address with fetched_coin_balance
  Repo.insert!(
    %Address{
      hash: addr,
      fetched_coin_balance: balance
    },
    on_conflict: [set: [fetched_coin_balance: balance]],
    conflict_target: :hash
  )

  Logger.info("Inserted/updated #{addr} with balance #{balance}")
end)

Logger.info("✅ Genesis balances loaded successfully!")