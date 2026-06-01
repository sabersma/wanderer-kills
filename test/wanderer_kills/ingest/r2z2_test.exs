defmodule WandererKills.Ingest.R2Z2Test do
  use ExUnit.Case, async: false

  alias WandererKills.Core.EtsOwner
  alias WandererKills.Ingest.R2Z2

  setup do
    table = EtsOwner.wanderer_kills_stats_table()

    # Ensure the ETS table exists - create it if the application supervisor
    # has been restarted or the EtsOwner process is not running
    if :ets.info(table) == :undefined do
      :ets.new(table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])
    end

    # Clean up any stale state before each test
    :ets.delete(table, :r2z2_last_sequence_id)
    :ets.delete(table, :r2z2_circuit_status)
    :ets.delete(table, :r2z2_stats)

    on_exit(fn ->
      if :ets.info(table) != :undefined do
        :ets.delete(table, :r2z2_last_sequence_id)
        :ets.delete(table, :r2z2_circuit_status)
        :ets.delete(table, :r2z2_stats)
      end
    end)

    %{table: table}
  end

  describe "base_url/0" do
    test "returns configured R2Z2 base URL" do
      url = R2Z2.base_url()
      assert is_binary(url)
      assert String.contains?(url, "r2z2.zkillboard.com")
    end
  end

  describe "get_circuit_status_cached/0" do
    test "returns default status when no ETS entry exists" do
      {:ok, status} = R2Z2.get_circuit_status_cached()
      assert status.circuit_state == :unknown
      assert status.consecutive_errors == 0
      assert status.cached == true
    end

    test "returns stored status from ETS", %{table: table} do
      stored = %{
        circuit_state: :open,
        consecutive_errors: 5,
        max_consecutive_errors: 10,
        circuit_opened_at: System.monotonic_time(:millisecond),
        circuit_reset_timeout_ms: 300_000,
        last_updated: System.system_time(:millisecond)
      }

      :ets.insert(table, {:r2z2_circuit_status, stored})

      {:ok, status} = R2Z2.get_circuit_status_cached()
      assert status.circuit_state == :open
      assert status.consecutive_errors == 5
    end

    test "returns partially populated ETS entry as-is", %{table: table} do
      # Only :consecutive_errors present — function returns the map verbatim
      partial = %{consecutive_errors: 3}
      :ets.insert(table, {:r2z2_circuit_status, partial})

      {:ok, status} = R2Z2.get_circuit_status_cached()
      assert status.consecutive_errors == 3
      # Missing keys are simply absent (no backfill)
      refute Map.has_key?(status, :circuit_state)
      refute Map.has_key?(status, :cached)
    end

    test "returns ETS entry with unexpected data types as-is", %{table: table} do
      # Insert values whose types don't match the normal schema
      invalid = %{
        circuit_state: "not_an_atom",
        consecutive_errors: "five",
        last_updated: :not_a_timestamp
      }

      :ets.insert(table, {:r2z2_circuit_status, invalid})

      # The function does not validate — it returns whatever ETS holds
      {:ok, status} = R2Z2.get_circuit_status_cached()
      assert status.circuit_state == "not_an_atom"
      assert status.consecutive_errors == "five"
      assert status.last_updated == :not_a_timestamp
    end
  end

  describe "sequence persistence via ETS" do
    test "sequence ID can be stored and retrieved", %{table: table} do
      :ets.insert(table, {:r2z2_last_sequence_id, 12345})

      [{:r2z2_last_sequence_id, id}] = :ets.lookup(table, :r2z2_last_sequence_id)
      assert id == 12345
    end
  end

  describe "ETS stats storage" do
    test "R2Z2 stats can be stored and retrieved", %{table: table} do
      stats = %{
        killmails_received: 5,
        killmails_older: 2,
        killmails_skipped: 1,
        errors: 0,
        no_kills_count: 10,
        circuit_open_skips: 0,
        total_killmails_received: 50,
        total_killmails_older: 20,
        total_killmails_skipped: 10,
        total_errors: 3,
        total_no_kills_count: 100,
        total_circuit_open_skips: 0
      }

      :ets.insert(table, {:r2z2_stats, stats})

      [{:r2z2_stats, stored}] = :ets.lookup(table, :r2z2_stats)
      assert stored.killmails_received == 5
      assert stored.total_killmails_received == 50
    end
  end
end
