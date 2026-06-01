defmodule WandererKills.Http.ConnectionMonitor do
  @stale_connection_threshold_seconds 300
  @recycle_interval_seconds Application.compile_env(
                              :wanderer_kills,
                              [:http, :conn_recycle_interval_seconds],
                              300
                            )
  @conn_max_idle_time Application.compile_env(
                        :wanderer_kills,
                        [:http, :conn_max_idle_time_ms],
                        90_000
                      )
  @moduledoc """
  Monitors HTTP connection pool health and can trigger connection recycling
  when issues are detected.
  """

  use GenServer
  require Logger

  @config Application.compile_env(:wanderer_kills, :http_connection_monitor, %{
            check_interval_ms: 60_000
          })
  @check_interval_ms @config[:check_interval_ms] || 60_000
  @error_threshold 5

  defmodule State do
    @moduledoc false
    defstruct [
      :consecutive_timeout_errors,
      :consecutive_connection_failures,
      :last_successful_request,
      :last_recycled_at,
      :total_timeouts,
      :total_connection_failures,
      :recycled_count,
      :failure_reasons
    ]
  end

  # Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Reports a timeout error for tracking.
  """
  def report_timeout(url) do
    GenServer.cast(__MODULE__, {:timeout_error, url})
  end

  @doc """
  Reports a successful request.
  """
  def report_success(url) do
    GenServer.cast(__MODULE__, {:success, url})
  end

  @doc """
  Reports a connection failure (non-timeout) for tracking.
  """
  def report_failure(url, reason) do
    GenServer.cast(__MODULE__, {:connection_failure, url, reason})
  end

  @doc """
  Gets the current monitor status.
  """
  def get_status do
    GenServer.call(__MODULE__, :get_status)
  end

  @doc """
  Manually triggers connection pool recycling.
  """
  def recycle_connections do
    GenServer.call(__MODULE__, :recycle_connections)
  end

  # GenServer callbacks

  @impl true
  def init(_opts) do
    Logger.info("[ConnectionMonitor] Starting HTTP connection monitor")

    schedule_check()

    state = %State{
      consecutive_timeout_errors: 0,
      consecutive_connection_failures: 0,
      last_successful_request: System.system_time(:second),
      last_recycled_at: System.system_time(:second),
      total_timeouts: 0,
      total_connection_failures: 0,
      recycled_count: 0,
      failure_reasons: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:timeout_error, url}, state) do
    new_consecutive = state.consecutive_timeout_errors + 1
    new_total = state.total_timeouts + 1

    Logger.warning(
      "[ConnectionMonitor] Timeout error reported",
      url: url,
      consecutive_errors: new_consecutive,
      total_timeouts: new_total
    )

    new_state = %State{
      state
      | consecutive_timeout_errors: new_consecutive,
        total_timeouts: new_total
    }

    if new_consecutive >= @error_threshold do
      Logger.error(
        "[ConnectionMonitor] Error threshold reached, recycling connections",
        consecutive_errors: new_consecutive
      )

      recycle_connection_pool()

      {:noreply,
       %State{
         new_state
         | consecutive_timeout_errors: 0,
           recycled_count: new_state.recycled_count + 1
       }}
    else
      {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast({:connection_failure, url, reason}, state) do
    new_consecutive = state.consecutive_connection_failures + 1
    new_total = state.total_connection_failures + 1

    new_failure_reasons = Map.update(state.failure_reasons, reason, 1, &(&1 + 1))

    Logger.warning(
      "[ConnectionMonitor] Connection failure reported",
      url: url,
      reason: reason,
      consecutive_failures: new_consecutive,
      total_failures: new_total
    )

    new_state = %State{
      state
      | consecutive_connection_failures: new_consecutive,
        total_connection_failures: new_total,
        failure_reasons: new_failure_reasons
    }

    if new_consecutive >= @error_threshold do
      Logger.error(
        "[ConnectionMonitor] Connection failure threshold reached, recycling connections",
        consecutive_failures: new_consecutive
      )

      recycle_connection_pool()

      {:noreply,
       %State{
         new_state
         | consecutive_connection_failures: 0,
           recycled_count: new_state.recycled_count + 1
       }}
    else
      {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast({:success, _url}, state) do
    new_state = %State{
      state
      | consecutive_timeout_errors: 0,
        consecutive_connection_failures: 0,
        last_successful_request: System.system_time(:second)
    }

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:check_health, state) do
    current_time = System.system_time(:second)
    time_since_success = current_time - state.last_successful_request
    time_since_recycled = current_time - state.last_recycled_at

    recycle_interval = @recycle_interval_seconds

    if time_since_success > @stale_connection_threshold_seconds do
      Logger.warning(
        "[ConnectionMonitor] No successful requests in #{time_since_success} seconds",
        action: "monitoring"
      )
    end

    new_state =
      if time_since_recycled >= recycle_interval do
        recycle_connection_pool()

        %State{
          state
          | last_recycled_at: current_time,
            recycled_count: state.recycled_count + 1,
            consecutive_timeout_errors: 0,
            consecutive_connection_failures: 0
        }
      else
        state
      end

    schedule_check()

    {:noreply, new_state}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    current_time = System.system_time(:second)

    status = %{
      consecutive_timeout_errors: state.consecutive_timeout_errors,
      consecutive_connection_failures: state.consecutive_connection_failures,
      last_successful_request: state.last_successful_request,
      last_recycled_at: state.last_recycled_at,
      total_timeouts: state.total_timeouts,
      total_connection_failures: state.total_connection_failures,
      recycled_count: state.recycled_count,
      time_since_success: current_time - state.last_successful_request,
      time_since_recycled: current_time - state.last_recycled_at,
      failure_reasons: state.failure_reasons,
      conn_max_idle_time: @conn_max_idle_time,
      recycle_interval: @recycle_interval_seconds
    }

    {:reply, status, state}
  end

  @impl true
  def handle_call(:recycle_connections, _from, state) do
    Logger.info("[ConnectionMonitor] Manual connection recycling requested")
    recycle_connection_pool()

    new_state = %State{
      state
      | consecutive_timeout_errors: 0,
        recycled_count: state.recycled_count + 1,
        last_recycled_at: System.system_time(:second)
    }

    {:reply, :ok, new_state}
  end

  defp schedule_check do
    Process.send_after(self(), :check_health, @check_interval_ms)
  end

  defp recycle_connection_pool do
    # Finch 0.20.0 has built-in connection recycling via conn_max_idle_time.
    # Restarting the entire Finch process is too aggressive and causes race
    # conditions where requests fail during the restart window. Instead we
    # rely on Finch's built-in idle connection management (configured with
    # conn_max_idle_time) and only emit telemetry here so monitors can track
    # recycling events.
    :telemetry.execute(
      [:wanderer_kills, :connection_monitor, :recycle],
      %{count: 1, conn_max_idle_time: @conn_max_idle_time},
      %{reason: :proactive}
    )

    :telemetry.execute(
      [:wanderer_kills, :http, :connection_recycled],
      %{count: 1},
      %{}
    )
  end
end
