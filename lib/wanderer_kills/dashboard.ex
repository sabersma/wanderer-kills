defmodule WandererKills.Dashboard do
  @moduledoc """
  Context module for dashboard data gathering and preparation.

  Handles fetching and formatting all data needed for the dashboard display,
  including health checks, system status, websocket statistics, ETS storage info,
  and R2Z2 ingest processing metrics.
  """

  require Logger

  alias WandererKills.Core.Cache
  alias WandererKills.Core.EtsOwner
  alias WandererKills.Core.Observability.Monitoring
  alias WandererKills.Ingest.Historical.HistoricalStreamer
  alias WandererKills.Utils

  @ets_tables Application.compile_env(:wanderer_kills, [:dashboard, :ets_tables], [
                {:killmails, "🗂️", "Killmails"},
                {:system_killmails, "🌌", "System Index"},
                {:system_kill_counts, "📊", "Kill Counts"},
                {:system_fetch_timestamps, "⏰", "Fetch Times"},
                {:killmail_events, "📝", "Events"},
                {:client_offsets, "🔖", "Client Offsets"},
                {:counters, "🔢", "Counters"}
              ])

  # R2Z2 reports stats every 60 seconds. We consider data stale if it's older than
  # 70 seconds to allow for some processing delay and clock drift
  @ingest_window_staleness_seconds 70

  @doc """
  Gathers all dashboard data needed for display.

  Returns a map containing:
  - `:status` - Unified system status information
  - `:health` - Application and service health data
  - `:websocket_stats` - WebSocket connection and message statistics
  - `:uptime` - Formatted uptime string
  - `:version` - Application version
  - `:ets_stats` - ETS table statistics
  - `:ingest_stats` - R2Z2 ingest processing statistics

  ## Returns

  - `{:ok, data}` - Successfully gathered all dashboard data
  - `{:error, reason}` - Failed to gather data due to an error
  """
  @spec get_dashboard_data() :: {:ok, map()} | {:error, String.t()}
  def get_dashboard_data do
    status = Monitoring.get_unified_status()

    memory_bytes = get_in(status, [:system, :memory, :total]) || 0
    memory_mb = if memory_bytes > 0, do: Float.round(memory_bytes / 1024 / 1024, 2), else: 0.0
    process_count = get_in(status, [:system, :processes, :count]) || 0

    app_health = %{
      status: "healthy",
      metrics: %{
        memory_mb: memory_mb,
        process_count: process_count,
        scheduler_usage: calculate_scheduler_usage(),
        uptime_seconds: get_in(status, [:system, :uptime_seconds]) || uptime_seconds()
      }
    }

    cache_stats = Cache.stats()
    cache_size = Cache.size()

    cache_health = %{
      status: "healthy",
      metrics: %{
        hit_rate: calculate_hit_rate(cache_stats),
        size: cache_size,
        eviction_count: 0,
        expiration_count: 0
      }
    }

    health = %{
      application: app_health,
      cache: cache_health
    }

    IO.puts(
      "[DASHBOARD FINAL] App memory: #{memory_mb} MB, processes: #{process_count}, cache size: #{cache_size}"
    )

    raw_websocket_stats = Utils.safe_get(status, [:websocket], %{})
    websocket_stats = raw_websocket_stats

    uptime = format_uptime(Utils.safe_get(status, [:system, :uptime_seconds], 0))

    version =
      case Application.spec(:wanderer_kills, :vsn) do
        nil -> "unknown"
        vsn -> to_string(vsn)
      end

    ets_stats = get_ets_stats()
    ingest_stats = get_ingest_stats(status)
    historical_stats = get_historical_streaming_stats()

    data = %{
      status: status,
      health: health,
      websocket_stats: websocket_stats,
      uptime: uptime,
      version: version,
      ets_stats: ets_stats,
      ingest_stats: ingest_stats,
      historical_stats: historical_stats
    }

    {:ok, data}
  rescue
    e ->
      Logger.error("Error gathering dashboard data: #{inspect(e)} - #{inspect(__STACKTRACE__)}")
      {:error, "Failed to gather dashboard data"}
  end

  defp calculate_hit_rate(stats) when is_map(stats) do
    # Handle different possible formats
    hits = stats[:hits] || stats["hits"] || 0
    misses = stats[:misses] || stats["misses"] || 0

    case {hits, misses} do
      {h, m} when h + m > 0 ->
        Float.round(h / (h + m) * 100, 2)

      _ ->
        0.0
    end
  end

  defp calculate_scheduler_usage do
    # Get scheduler utilization (this is an approximation)
    run_queue = :erlang.statistics(:run_queue)
    schedulers = :erlang.system_info(:schedulers)

    usage = run_queue / schedulers * 100
    Float.round(min(usage, 100.0), 1)
  rescue
    _ -> 0.0
  end

  defp format_uptime(seconds) when is_number(seconds) do
    days = div(seconds, 86_400)
    hours = div(rem(seconds, 86_400), 3600)
    minutes = div(rem(seconds, 3600), 60)

    cond do
      days > 0 -> "#{days}d #{hours}h #{minutes}m"
      hours > 0 -> "#{hours}h #{minutes}m"
      true -> "#{minutes}m"
    end
  end

  defp format_uptime(_), do: "N/A"

  defp get_ets_stats do
    Enum.map(@ets_tables, fn {table, icon, name} ->
      try do
        info = :ets.info(table)
        size = Keyword.get(info, :size, 0)
        memory = Keyword.get(info, :memory, 0)
        # Convert words to bytes using dynamic word size for cross-platform compatibility
        word_size = :erlang.system_info(:wordsize)
        memory_bytes = memory * word_size
        memory_mb = Float.round(memory_bytes / (1024 * 1024), 2)

        %{
          name: name,
          icon: icon,
          table: table,
          size: size,
          memory_mb: memory_mb,
          available: true
        }
      rescue
        _ ->
          %{
            name: name,
            icon: icon,
            table: table,
            size: 0,
            memory_mb: 0,
            available: false
          }
      end
    end)
  end

  defp get_ingest_stats(status) do
    total_processed = Utils.safe_get(status, [:processing, :killmails_received], 0)

    last_killmail_ago =
      Utils.safe_get(status, [:processing, :last_killmail_ago_seconds], nil)

    processing_lag = Utils.safe_get(status, [:processing, :processing_lag_seconds], 0)

    processing_rate = calculate_current_processing_rate(status)
    last_processed = format_last_processed_time(last_killmail_ago)

    %{
      total_processed: total_processed,
      processing_rate: processing_rate,
      last_processed: last_processed,
      queue_lag: round(processing_lag * 1000),
      status: if(total_processed > 0, do: "active", else: "idle")
    }
  end

  defp calculate_current_processing_rate(status) do
    # Get the raw R2Z2 stats from ETS for more accurate rate calculation
    case :ets.info(EtsOwner.wanderer_kills_stats_table()) do
      :undefined ->
        total = Utils.safe_get(status, [:processing, :killmails_received], 0)
        calculate_simple_rate_from_total(total)

      _ ->
        calculate_rate_from_ets_stats(status)
    end
  end

  defp calculate_rate_from_ets_stats(status) do
    case :ets.lookup(EtsOwner.wanderer_kills_stats_table(), :r2z2_stats) do
      [{:r2z2_stats, stats}] when is_map(stats) ->
        calculate_rate_from_window_stats(stats)

      _ ->
        total = Utils.safe_get(status, [:processing, :killmails_received], 0)
        calculate_simple_rate_from_total(total)
    end
  end

  defp calculate_rate_from_window_stats(stats) do
    # R2Z2 tracks killmails_received in 60-second windows
    # Use the window stats for a more accurate current rate
    window_kills = Map.get(stats, :killmails_received, 0)
    last_reset = Map.get(stats, :last_reset, DateTime.utc_now())

    seconds_elapsed = DateTime.diff(DateTime.utc_now(), last_reset, :second)

    if seconds_elapsed > 0 and seconds_elapsed <= @ingest_window_staleness_seconds do
      round(window_kills * 60 / seconds_elapsed)
    else
      total = Map.get(stats, :total_killmails_received, 0)
      calculate_simple_rate_from_total(total)
    end
  end

  defp calculate_simple_rate_from_total(total_processed) do
    uptime_minutes = max(1, div(uptime_seconds(), 60))
    round(total_processed / uptime_minutes)
  end

  defp format_last_processed_time(nil), do: "Never"

  defp format_last_processed_time(seconds_ago) when is_number(seconds_ago) do
    cond do
      seconds_ago >= 999_999 -> "Never"
      seconds_ago < 60 -> "#{round(seconds_ago)} seconds ago"
      seconds_ago < 3600 -> "#{round(seconds_ago / 60)} minutes ago"
      seconds_ago < 86_400 -> "#{round(seconds_ago / 3600)} hours ago"
      true -> "#{round(seconds_ago / 86_400)} days ago"
    end
  end

  defp format_last_processed_time(_value), do: "Unknown"

  # Returns the Erlang VM's wall clock time since start (not the specific application runtime)
  # This represents how long the BEAM VM has been running, which may be longer than
  # the actual application uptime if the VM was started before the application
  defp uptime_seconds do
    {uptime_ms, _} = :erlang.statistics(:wall_clock)
    div(uptime_ms, 1000)
  end

  # Gets historical streaming statistics from the HistoricalStreamer process.
  #
  # Returns statistics about the current historical streaming progress including:
  # - Current date being processed
  # - Total processed count
  # - Failed count
  # - Progress percentage
  # - Current status (running/paused)
  defp get_historical_streaming_stats do
    config = Application.get_env(:wanderer_kills, :historical_streaming, %{})

    if Keyword.get(config, :enabled, false) do
      get_enabled_streaming_stats(config)
    else
      format_disabled_streaming_stats()
    end
  end

  defp get_enabled_streaming_stats(config) do
    case safely_get_streaming_status() do
      {:ok, status} ->
        format_running_streaming_stats(status, config)

      {:error, :not_started} ->
        format_not_started_streaming_stats()

      {:error, :process_exited} ->
        format_error_streaming_stats("Process exited")

      {:error, _reason} ->
        format_error_streaming_stats("Error getting status")
    end
  end

  defp safely_get_streaming_status do
    streamer_module = HistoricalStreamer

    case GenServer.whereis(streamer_module) do
      nil ->
        {:error, :not_started}

      pid when is_pid(pid) ->
        try do
          case streamer_module.status() do
            %{} = status -> {:ok, status}
            _ -> {:error, :invalid_status}
          end
        catch
          :exit, {:noproc, _} ->
            {:error, :process_exited}

          :exit, reason ->
            {:error, {:exit, reason}}

          _, _ ->
            {:error, :unknown_error}
        end
    end
  end

  defp format_running_streaming_stats(status, config) do
    %{
      enabled: true,
      running: true,
      status: if(status.paused, do: "Paused", else: "Running"),
      progress: status.progress_percentage,
      current_date: status.current_date,
      processed_count: status.processed_count,
      failed_count: status.failed_count,
      queue_size: status.queue_size,
      start_date: Keyword.get(config, :start_date, "unknown"),
      end_date: status.end_date
    }
  end

  defp format_not_started_streaming_stats do
    %{
      enabled: true,
      running: false,
      status: "Not started",
      progress: 0.0,
      current_date: nil,
      processed_count: 0,
      failed_count: 0,
      queue_size: 0
    }
  end

  defp format_error_streaming_stats(error_message) do
    %{
      enabled: true,
      running: false,
      status: error_message,
      progress: 0.0,
      current_date: nil,
      processed_count: 0,
      failed_count: 0,
      queue_size: 0
    }
  end

  defp format_disabled_streaming_stats do
    %{
      enabled: false,
      running: false,
      status: "Disabled",
      progress: 0.0,
      current_date: nil,
      processed_count: 0,
      failed_count: 0,
      queue_size: 0
    }
  end
end
