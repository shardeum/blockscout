defmodule Explorer.Repo.Migrations.CleanupDuplicateCosmosTransactions do
  use Ecto.Migration

  @doc """
  This migration cleans up duplicate Cosmos transactions that have a corresponding EVM transaction.

  When the same transaction is indexed through both the Cosmos fetcher and the EVM indexer,
  it results in two entries in the transactions table:
  1. A Cosmos transaction with transaction_type = 'cosmos' and alt_hash set to the EVM hash
  2. An EVM transaction with the EVM hash as its primary hash

  This migration deletes the Cosmos transaction entries where a corresponding EVM transaction exists,
  keeping only the EVM transaction to prevent duplicate display in the explorer.
  """

  def up do
    # Delete Cosmos transactions where their alt_hash matches an existing EVM transaction's hash
    execute("""
    DELETE FROM transactions
    WHERE transaction_type = 'cosmos'
      AND alt_hash IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM transactions evm_tx
        WHERE evm_tx.hash = transactions.alt_hash
          AND (evm_tx.transaction_type IS NULL OR evm_tx.transaction_type != 'cosmos')
      )
    """)
  end

  def down do
    # Cannot restore deleted transactions
    :ok
  end
end
