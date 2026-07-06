defmodule Explorer.Repo.Migrations.AddSdkTxHashToTransactions do
  use Ecto.Migration

  def change do
    alter table(:transactions) do
      # Raw SDK (Cosmos) transaction hash, decoded from the hex string returned
      # by the dashboard API, kept separate from `hash` (which may be a
      # synthetic digest) so the original hash can be displayed and searched.
      add(:sdk_tx_hash, :bytea, null: true)
    end

    create(index(:transactions, :sdk_tx_hash))
  end
end
