defmodule WandererKills.Core.EtsOwner do
  @moduledoc """
  Owns ETS tables for the application.

  This GenServer ensures ETS tables are created at application start
  and are properly owned by a long-lived process to prevent data loss.
  """

  use GenServer
  require Logger

  @websocket_stats_table :websocket_stats
  @wanderer_kills_stats_table :wanderer_kills_stats

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    Logger.info("Starting ETS Owner")

    # Create the websocket stats table with concurrency optimizations
    # Handle the case where table might already exist during hot-code reloads
    case :ets.info(@websocket_stats_table) do
      :undefined ->
        :ets.new(@websocket_stats_table, [
          :named_table,
          :public,
          :set,
          read_concurrency: true,
          write_concurrency: true
        ])

        :ets.insert(@websocket_stats_table, {:kills_sent_realtime, 0})
        :ets.insert(@websocket_stats_table, {:kills_sent_preload, 0})
        :ets.insert(@websocket_stats_table, {:last_reset, DateTime.utc_now()})

      _ ->
        Logger.debug("ETS table #{@websocket_stats_table} already exists, skipping creation")
    end

    # Create the wanderer_kills_stats table for unified status reporting
    case :ets.info(@wanderer_kills_stats_table) do
      :undefined ->
        :ets.new(@wanderer_kills_stats_table, [
          :named_table,
          :public,
          :set,
          read_concurrency: true,
          write_concurrency: true
        ])

        # Pre-seed with default values to prevent crashes on early reads
        now = DateTime.utc_now()

        # Initialize R2Z2 stats
        :ets.insert(
          @wanderer_kills_stats_table,
          {:r2z2_stats,
           %{
             killmails_received: 0,
             killmails_older: 0,
             killmails_skipped: 0,
             errors: 0,
             no_kills_count: 0,
             circuit_open_skips: 0,
             last_reset: now,
             last_killmail_received_at: nil,
             systems_active: MapSet.new(),
             total_killmails_received: 0,
             total_killmails_older: 0,
             total_killmails_skipped: 0,
             total_errors: 0,
             total_no_kills_count: 0,
             total_circuit_open_skips: 0
           }}
        )

        # Initialize parser stats
        :ets.insert(
          @wanderer_kills_stats_table,
          {:parser_stats,
           %{
             stored: 0,
             skipped: 0,
             failed: 0,
             total_processed: 0,
             last_reset: now
           }}
        )

        # Initialize WebSocket stats
        :ets.insert(
          @wanderer_kills_stats_table,
          {:websocket_stats,
           %{
             kills_sent: %{realtime: 0, preload: 0, total: 0},
             connections: %{active: 0, total_connected: 0, total_disconnected: 0},
             subscriptions: %{active: 0, total_added: 0, total_removed: 0, total_systems: 0},
             rates: %{},
             uptime_seconds: 0,
             started_at: now,
             last_reset: now,
             timestamp: now
           }}
        )

      _ ->
        Logger.debug("ETS table #{@wanderer_kills_stats_table} already exists, skipping creation")
    end

    Logger.info("ETS tables initialized successfully")

    {:ok, %{tables: [@websocket_stats_table, @wanderer_kills_stats_table]}}
  end

  @doc """
  Get the websocket stats table name.
  """
  @spec websocket_stats_table() :: atom()
  def websocket_stats_table, do: @websocket_stats_table

  @doc """
  Get the wanderer kills stats table name.
  """
  @spec wanderer_kills_stats_table() :: atom()
  def wanderer_kills_stats_table, do: @wanderer_kills_stats_table
end
