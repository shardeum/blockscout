defmodule Explorer.Repo.Migrations.AddCosmosTransactionSupport do
  use Ecto.Migration

  def up do
    # Create transaction_type enum
    execute("""
    CREATE TYPE transaction_type AS ENUM ('evm', 'cosmos');
    """)

    # Add transaction_type column with default 'evm' for backward compatibility
    alter table(:transactions) do
      add(:transaction_type, :transaction_type, default: "evm", null: false)
      # Store Cosmos-specific data as JSONB
      # This includes: messages, fees, memo, gas info, signatures, etc.
      add(:cosmos_data, :jsonb, null: true)
      # Alternative hash for cross-chain mapping (e.g., EVM hash for Cosmos tx)
      add(:alt_hash, :bytea, null: true)
    end

    # Create index on transaction_type for efficient filtering
    create(index(:transactions, :transaction_type))

    # Create index on alt_hash for cross-chain lookups
    create(index(:transactions, :alt_hash))

    # Modify constraints to allow null values for cosmos transactions
    # Cosmos transactions don't use EVM signature fields (r, s, v)
    execute("""
    ALTER TABLE transactions
    DROP CONSTRAINT IF EXISTS transactions_r_not_null;
    """)

    execute("""
    ALTER TABLE transactions
    DROP CONSTRAINT IF EXISTS transactions_s_not_null;
    """)

    execute("""
    ALTER TABLE transactions
    DROP CONSTRAINT IF EXISTS transactions_v_not_null;
    """)

    # Add new constraints that allow null for cosmos transactions
    create(
      constraint(
        :transactions,
        :evm_signature_fields,
        check: """
        (transaction_type = 'cosmos') OR
        (transaction_type = 'evm' AND r IS NOT NULL AND s IS NOT NULL AND v IS NOT NULL)
        """
      )
    )

    # Modify gas_price constraint for cosmos (can be null)
    execute("""
    ALTER TABLE transactions ALTER COLUMN gas_price DROP NOT NULL;
    """)

    create(
      constraint(
        :transactions,
        :evm_gas_price,
        check: """
        (transaction_type = 'cosmos') OR
        (transaction_type = 'evm' AND gas_price IS NOT NULL)
        """
      )
    )

    # Modify input constraint for cosmos (can be null)
    execute("""
    ALTER TABLE transactions ALTER COLUMN input DROP NOT NULL;
    """)

    create(
      constraint(
        :transactions,
        :evm_input,
        check: """
        (transaction_type = 'cosmos') OR
        (transaction_type = 'evm' AND input IS NOT NULL)
        """
      )
    )

    # Modify nonce constraint for cosmos (can be null)
    execute("""
    ALTER TABLE transactions ALTER COLUMN nonce DROP NOT NULL;
    """)

    create(
      constraint(
        :transactions,
        :evm_nonce,
        check: """
        (transaction_type = 'cosmos') OR
        (transaction_type = 'evm' AND nonce IS NOT NULL)
        """
      )
    )
  end

  def down do
    execute("""
    ALTER TABLE transactions DROP CONSTRAINT IF EXISTS evm_signature_fields;
    """)

    execute("""
    ALTER TABLE transactions DROP CONSTRAINT IF EXISTS evm_gas_price;
    """)

    execute("""
    ALTER TABLE transactions DROP CONSTRAINT IF EXISTS evm_input;
    """)

    execute("""
    ALTER TABLE transactions DROP CONSTRAINT IF EXISTS evm_nonce;
    """)

    # Restore NOT NULL constraints (this will fail if there are cosmos transactions)
    execute("""
    ALTER TABLE transactions ALTER COLUMN nonce SET NOT NULL;
    """)

    execute("""
    ALTER TABLE transactions ALTER COLUMN input SET NOT NULL;
    """)

    execute("""
    ALTER TABLE transactions ALTER COLUMN gas_price SET NOT NULL;
    """)

    drop(index(:transactions, :alt_hash))
    drop(index(:transactions, :transaction_type))

    alter table(:transactions) do
      remove(:alt_hash)
      remove(:cosmos_data)
      remove(:transaction_type)
    end

    execute("""
    DROP TYPE transaction_type;
    """)
  end
end
