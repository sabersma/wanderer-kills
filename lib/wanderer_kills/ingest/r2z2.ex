defmodule WandererKills.Ingest.R2Z2 do
  @moduledoc """
  Client for interacting with the zKillboard R2Z2 API.

  R2Z2 uses a sequence-based polling model:

  • Init:           fetch starting sequence from sequence.json
  • On kill (200):  parse full killmail + zkb, increment sequence, poll fast
  • On 404:         no more killmails, wait ≥6s before retrying same sequence
  • On 429:         rate limited, respect Retry-After or back off
  • On 403:         blocked, circuit breaker open, long backoff
  • On error:       exponential backoff up to :max_backoff_ms

  R2Z2 returns full ESI killmail data + zkb metadata in a single response,
  so no separate ESI fetch is needed.
  """

  use GenServer
  require Logger

  alias WandererKills.Core.EtsOwner
  alias WandererKills.Core.Support.Error
  alias WandererKills.Core.Support.SupervisedTask
  alias WandererKills.Core.Support.Utils
  alias WandererKills.Domain.Killmail
  alias WandererKills.Http.Client, as: HttpClient
  alias WandererKills.Ingest.Killmails.UnifiedProcessor
  alias WandererKills.Subs.Broadcaster
  alias WandererKills.Subs.SimpleSubscriptionManager, as: SubscriptionManager

  @user_agent "(wanderer-kills@proton.me; +https://github.com/wanderer-industries/wanderer-kills)"

  # Runtime configuration defaults
  @default_config %{
    base_url: "https://r2z2.zkillboard.com/ephemeral",
    poll_interval_ms: 100,
    idle_interval_ms: 6_000,
    initial_backoff_ms: 1_000,
    max_backoff_ms: 60_000,
    backoff_factor: 2,
    task_timeout_ms: 10_000,
    max_consecutive_errors: 10,
    # 5 minutes
    circuit_reset_timeout_ms: 300_000
  }

  defmodule State do
    @moduledoc false
    defstruct [
      :sequence_id,
      :backoff_ms,
      :stats,
      :consecutive_errors,
      :circuit_state,
      :circuit_opened_at,
      sequence_retry_count: 0
    ]
  end

  # Runtime configuration loader — reads Application env so values
  # can be changed without recompilation.
  defp load_config do
    r2z2 = Application.get_env(:wanderer_kills, :r2z2, [])

    %{
      base_url: Keyword.get(r2z2, :base_url, @default_config.base_url),
      poll_interval_ms: Keyword.get(r2z2, :poll_interval_ms, @default_config.poll_interval_ms),
      idle_interval_ms: Keyword.get(r2z2, :idle_interval_ms, @default_config.idle_interval_ms),
      initial_backoff_ms:
        Keyword.get(r2z2, :initial_backoff_ms, @default_config.initial_backoff_ms),
      max_backoff_ms: Keyword.get(r2z2, :max_backoff_ms, @default_config.max_backoff_ms),
      backoff_factor: Keyword.get(r2z2, :backoff_factor, @default_config.backoff_factor),
      task_timeout_ms: Keyword.get(r2z2, :task_timeout_ms, @default_config.task_timeout_ms),
      max_consecutive_errors:
        Keyword.get(r2z2, :max_consecutive_errors, @default_config.max_consecutive_errors),
      circuit_reset_timeout_ms:
        Keyword.get(r2z2, :circuit_reset_timeout_ms, @default_config.circuit_reset_timeout_ms)
    }
  end

  #
  # Public API
  #

  @doc """
  Gets the base URL for R2Z2 API calls.
  """
  @spec base_url() :: String.t()
  def base_url do
    load_config().base_url
  end

  @doc """
  Starts the R2Z2 worker as a GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    Logger.info("[R2Z2] Starting R2Z2 worker")
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Gets current R2Z2 statistics.
  """
  @spec get_stats() :: map()
  def get_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  @doc """
  Gets current circuit breaker status.
  """
  @spec get_circuit_status() :: {:ok, map()}
  def get_circuit_status do
    get_circuit_status(5000)
  end

  @doc """
  Gets current circuit breaker status with custom timeout.
  """
  @spec get_circuit_status(timeout()) :: {:ok, map()}
  def get_circuit_status(timeout) do
    GenServer.call(__MODULE__, :get_circuit_status, timeout)
  end

  @doc """
  Gets circuit breaker status from ETS without blocking.
  Returns cached status or default if not available.
  """
  @spec get_circuit_status_cached() :: {:ok, map()}
  def get_circuit_status_cached do
    config = load_config()

    case :ets.lookup(EtsOwner.wanderer_kills_stats_table(), :r2z2_circuit_status) do
      [{:r2z2_circuit_status, status}] ->
        {:ok, status}

      _ ->
        {:ok,
         %{
           circuit_state: :unknown,
           consecutive_errors: 0,
           max_consecutive_errors: config.max_consecutive_errors,
           circuit_opened_at: nil,
           circuit_reset_timeout_ms: config.circuit_reset_timeout_ms,
           cached: true
         }}
    end
  end

  #
  # Server Callbacks
  #

  @impl true
  def init(_opts) do
    config = load_config()

    # Initialize statistics tracking
    stats = %{
      killmails_received: 0,
      killmails_older: 0,
      killmails_skipped: 0,
      errors: 0,
      no_kills_count: 0,
      circuit_open_skips: 0,
      last_reset: DateTime.utc_now(),
      last_killmail_received_at: nil,
      systems_active: MapSet.new(),
      # Cumulative stats that don't reset
      total_killmails_received: 0,
      total_killmails_older: 0,
      total_killmails_skipped: 0,
      total_errors: 0,
      total_no_kills_count: 0,
      total_circuit_open_skips: 0
    }

    state = %State{
      sequence_id: nil,
      backoff_ms: config.initial_backoff_ms,
      stats: stats,
      consecutive_errors: 0,
      circuit_state: :closed,
      circuit_opened_at: nil
    }

    {:ok, state, {:continue, :fetch_initial_sequence}}
  end

  @impl true
  def handle_continue(:fetch_initial_sequence, state) do
    Logger.info("[R2Z2] Fetching initial sequence ID")

    # Try to recover sequence from ETS first
    sequence_id = recover_sequence_from_ets()

    case sequence_id do
      nil ->
        # Fetch from R2Z2 API
        fetch_sequence_from_api(state)

      id ->
        Logger.info("[R2Z2] Recovered sequence ID from ETS: #{id}")
        start_polling(%State{state | sequence_id: id})
    end
  end

  @impl true
  def handle_info(:poll_kills, %State{} = state) do
    config = load_config()

    # Check circuit breaker state
    case check_circuit_breaker(state, config) do
      {:ok, state} ->
        # Circuit is closed or half-open, proceed with polling
        Logger.debug("[R2Z2] Polling R2Z2 (sequence: #{state.sequence_id})")
        result = do_poll(state.sequence_id, config)
        {:noreply, handle_poll_success(result, state, config)}

      {:circuit_open, state} ->
        # Circuit is open, skip polling and schedule retry
        Logger.warning("[R2Z2] Circuit breaker is OPEN - skipping poll")

        # Update stats to track circuit open skips
        new_stats = %{
          state.stats
          | circuit_open_skips: state.stats.circuit_open_skips + 1,
            total_circuit_open_skips: state.stats.total_circuit_open_skips + 1
        }

        # Update ETS immediately for real-time dashboard metrics
        update_ets_stats(new_stats)

        # Update circuit breaker status in ETS
        update_circuit_status_ets(
          state.circuit_state,
          state.consecutive_errors,
          state.circuit_opened_at,
          config
        )

        # Schedule a retry after circuit reset timeout
        schedule_poll(config.circuit_reset_timeout_ms)

        {:noreply, %State{state | stats: new_stats}}
    end
  end

  @impl true
  def handle_info(:log_summary, %State{stats: stats} = state) do
    log_summary(stats)

    # Reset stats and schedule next summary
    reset_stats = %{
      stats
      | killmails_received: 0,
        killmails_older: 0,
        killmails_skipped: 0,
        errors: 0,
        no_kills_count: 0,
        circuit_open_skips: 0,
        last_reset: DateTime.utc_now(),
        systems_active: MapSet.new()
        # Preserve last_killmail_received_at - it should persist across resets
    }

    schedule_summary_log()
    {:noreply, %State{state | stats: reset_stats}}
  end

  @impl true
  def handle_info({:track_system, system_id}, %State{stats: stats} = state) do
    new_stats = track_system_activity(stats, system_id)
    {:noreply, %State{state | stats: new_stats}}
  end

  @impl true
  def handle_info(:retry_sequence_fetch, state) do
    fetch_sequence_from_api(state)
  end

  defp handle_poll_success(result, %State{} = state, config) do
    # Update statistics based on result
    new_stats = update_stats(state.stats, result)

    # Update ETS immediately for real-time dashboard metrics
    update_ets_stats(new_stats)

    # Update error counter and circuit state
    {new_consecutive_errors, new_circuit_state, new_circuit_opened_at} =
      update_circuit_state(
        result,
        state.consecutive_errors,
        state.circuit_state,
        state.circuit_opened_at,
        config
      )

    # Update sequence ID on success
    new_sequence_id = update_sequence_id(result, state.sequence_id)

    {delay_ms, new_backoff} = next_schedule(result, state.backoff_ms, config)
    schedule_poll(delay_ms)

    # Persist sequence ID to ETS for restart recovery
    persist_sequence_to_ets(new_sequence_id)

    # Update circuit breaker status in ETS for non-blocking access
    update_circuit_status_ets(
      new_circuit_state,
      new_consecutive_errors,
      new_circuit_opened_at,
      config
    )

    %State{
      state
      | sequence_id: new_sequence_id,
        backoff_ms: new_backoff,
        stats: new_stats,
        consecutive_errors: new_consecutive_errors,
        circuit_state: new_circuit_state,
        circuit_opened_at: new_circuit_opened_at
    }
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    stats = %{
      killmails_received: state.stats.total_killmails_received,
      killmails_older: state.stats.total_killmails_older,
      killmails_skipped: state.stats.total_killmails_skipped,
      errors: state.stats.total_errors,
      no_kills_polls: state.stats.total_no_kills_count,
      active_systems: MapSet.size(state.stats.systems_active),
      total_polls:
        state.stats.total_killmails_received + state.stats.total_killmails_older +
          state.stats.total_killmails_skipped + state.stats.total_no_kills_count +
          state.stats.total_errors,
      last_reset: state.stats.last_reset,
      last_killmail_received_at: state.stats.last_killmail_received_at,
      current_sequence_id: state.sequence_id
    }

    {:reply, {:ok, stats}, state}
  end

  @impl true
  def handle_call(:get_circuit_status, _from, state) do
    config = load_config()

    status = %{
      circuit_state: state.circuit_state,
      consecutive_errors: state.consecutive_errors,
      max_consecutive_errors: config.max_consecutive_errors,
      circuit_opened_at: state.circuit_opened_at,
      circuit_reset_timeout_ms: config.circuit_reset_timeout_ms
    }

    {:reply, {:ok, status}, state}
  end

  #
  # Private Helpers
  #

  # Recover sequence ID from ETS (for restart recovery)
  defp recover_sequence_from_ets do
    if :ets.info(EtsOwner.wanderer_kills_stats_table()) != :undefined do
      case :ets.lookup(EtsOwner.wanderer_kills_stats_table(), :r2z2_last_sequence_id) do
        [{:r2z2_last_sequence_id, id}] when is_integer(id) -> id
        _ -> nil
      end
    else
      nil
    end
  end

  # Persist sequence ID to ETS for restart recovery
  defp persist_sequence_to_ets(sequence_id) when is_integer(sequence_id) do
    if :ets.info(EtsOwner.wanderer_kills_stats_table()) != :undefined do
      :ets.insert(EtsOwner.wanderer_kills_stats_table(), {:r2z2_last_sequence_id, sequence_id})
    end
  end

  defp persist_sequence_to_ets(_), do: :ok

  # Fetch the starting sequence from the R2Z2 API
  defp fetch_sequence_from_api(state) do
    url = "#{base_url()}/sequence.json"
    headers = [{"user-agent", @user_agent}]

    case HttpClient.get_r2z2(url, headers) do
      {:ok, %{body: body}} when is_map(body) ->
        # R2Z2 returns {"sequence": N} per API docs
        sequence_id = Map.get(body, "sequence") || Map.get(body, "sequence_id")

        if is_integer(sequence_id) do
          Logger.info("[R2Z2] Got initial sequence ID: #{sequence_id}")
          start_polling(%State{state | sequence_id: sequence_id})
        else
          Logger.error("[R2Z2] Could not parse sequence from response: #{inspect(body)}")
          retry_sequence_fetch(state)
        end

      {:ok, %{body: body}} when is_binary(body) ->
        case Integer.parse(body) do
          {sequence_id, _} ->
            Logger.info("[R2Z2] Got initial sequence ID from string: #{sequence_id}")
            start_polling(%State{state | sequence_id: sequence_id})

          :error ->
            Logger.error("[R2Z2] Could not parse sequence from string: #{inspect(body)}")
            retry_sequence_fetch(state)
        end

      {:error, reason} ->
        Logger.error("[R2Z2] Failed to fetch initial sequence: #{inspect(reason)}")
        retry_sequence_fetch(state)
    end
  end

  defp retry_sequence_fetch(state) do
    config = load_config()
    retry_count = state.sequence_retry_count

    delay =
      min(
        config.idle_interval_ms * Integer.pow(config.backoff_factor, retry_count),
        config.max_backoff_ms
      )

    Logger.warning(
      "[R2Z2] Will retry fetching sequence in #{delay}ms (attempt #{retry_count + 1})"
    )

    Process.send_after(self(), :retry_sequence_fetch, delay)
    {:noreply, %State{state | sequence_retry_count: retry_count + 1}}
  end

  defp start_polling(state) do
    config = load_config()
    Logger.info("[R2Z2] Starting polling with sequence ID: #{state.sequence_id}")

    # Reset sequence retry count on successful fetch
    state = %State{state | sequence_retry_count: 0}

    # Initialize circuit status in ETS
    update_circuit_status_ets(
      state.circuit_state,
      state.consecutive_errors,
      state.circuit_opened_at,
      config
    )

    # Persist initial sequence
    persist_sequence_to_ets(state.sequence_id)

    # Schedule the very first poll after a short delay
    schedule_poll(config.poll_interval_ms)
    # Schedule the first summary log
    schedule_summary_log()
    {:noreply, state}
  end

  # Schedules the next :poll_kills message in `ms` milliseconds.
  defp schedule_poll(ms) do
    Process.send_after(self(), :poll_kills, ms)
  end

  # Schedules the next :log_summary message in 60 seconds.
  defp schedule_summary_log do
    Process.send_after(self(), :log_summary, 60_000)
  end

  # Perform the actual HTTP GET + parsing and return one of:
  #   - {:ok, :kill_received, next_sequence_id}
  #   - {:ok, :no_kills}
  #   - {:ok, :kill_older, next_sequence_id}
  #   - {:ok, :kill_skipped, next_sequence_id}
  #   - {:error, reason}
  defp do_poll(sequence_id, config) do
    url = "#{base_url()}/#{sequence_id}.json"
    Logger.debug("[R2Z2] Starting poll request to: #{url}")

    headers = [{"user-agent", @user_agent}]
    start_time = System.monotonic_time(:millisecond)

    result = HttpClient.get_r2z2(url, headers)
    elapsed_ms = System.monotonic_time(:millisecond) - start_time

    Logger.debug("[R2Z2] HTTP request completed in #{elapsed_ms}ms")

    case result do
      {:ok, %{body: body}} when is_map(body) ->
        process_r2z2_response(body, config)

      {:error, %Error{type: :not_found}} ->
        # 404 - no more killmails at this sequence
        Logger.debug("[R2Z2] No killmail at sequence #{sequence_id} (404)")
        {:ok, :no_kills}

      {:error, %Error{type: :rate_limited} = error} ->
        # 429 - rate limited
        Logger.warning("[R2Z2] Rate limited: #{inspect(error)}")
        {:error, error}

      {:error, %Error{type: :client_error, details: %{status: 403}}} ->
        # 403 - blocked
        Logger.error("[R2Z2] Blocked by R2Z2 API (403)")
        {:error, Error.http_error(:forbidden, "Blocked by R2Z2 API", false)}

      {:error, reason} ->
        Logger.warning("[R2Z2] HTTP request failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Process a successful R2Z2 response containing full killmail data + zkb metadata.
  # The response shape is:
  #   {
  #     "killmail_id": 123456,
  #     "killmail_time": "2024-01-01T00:00:00Z",
  #     "solar_system_id": 30000142,
  #     "victim": {...},
  #     "attackers": [...],
  #     "zkb": { "hash": "abc...", "totalValue": 1234.56, ... },
  #     "sequence_id": 789
  #   }
  defp process_r2z2_response(body, config) do
    # Extract sequence_id from response for advancing the pointer
    next_sequence_id = Map.get(body, "sequence_id")

    # The body already contains full killmail data + zkb
    killmail_id = Map.get(body, "killmail_id")

    if killmail_id do
      Logger.debug("[R2Z2] Processing killmail #{killmail_id} (sequence: #{next_sequence_id})")
      process_killmail_with_task(body, killmail_id, next_sequence_id, config)
    else
      Logger.warning(
        "[R2Z2] Unexpected response shape (no killmail_id): #{inspect(Map.keys(body))}"
      )

      {:error,
       Error.invalid_format_error("Unexpected R2Z2 response format", %{keys: Map.keys(body)})}
    end
  end

  # Process a single killmail from R2Z2 inside a supervised async task.
  # Since R2Z2 returns full ESI data + zkb metadata, no ESI fetch is needed.
  defp process_killmail_with_task(body, killmail_id, next_sequence_id, config) do
    result =
      SupervisedTask.async(
        fn -> process_full_killmail(body) end,
        timeout: config.task_timeout_ms,
        task_name: "r2z2_kill_processing",
        metadata: %{kill_id: killmail_id, sequence_id: next_sequence_id}
      )

    case result do
      {:ok, {:ok, :kill_received}} ->
        Logger.debug("[R2Z2] Successfully processed kill #{killmail_id}")
        {:ok, :kill_received, next_sequence_id}

      {:ok, {:ok, :kill_older}} ->
        Logger.debug("[R2Z2] Kill #{killmail_id} is older than cutoff → skipping.")
        {:ok, :kill_older, next_sequence_id}

      {:ok, {:ok, :kill_skipped}} ->
        Logger.debug("[R2Z2] Kill #{killmail_id} already ingested → skipping.")
        {:ok, :kill_skipped, next_sequence_id}

      {:ok, {:error, reason}} ->
        Logger.error("[R2Z2] Kill #{killmail_id} processing failed: #{inspect(reason)}")
        {:error, reason}

      {:ok, other} ->
        Logger.error("[R2Z2] Unexpected task result for kill #{killmail_id}: #{inspect(other)}")

        {:error,
         Error.system_error(
           :unexpected_task_result,
           "Unexpected task result for kill processing",
           false,
           %{
             kill_id: killmail_id,
             result: other,
             sequence_id: next_sequence_id
           }
         )}

      {:error, :timeout} ->
        Logger.warning(
          "[R2Z2] Kill #{killmail_id} processing timed out after #{config.task_timeout_ms}ms"
        )

        {:error,
         Error.system_error(
           :timeout,
           "Processing timed out after #{config.task_timeout_ms}ms",
           true,
           %{kill_id: killmail_id, timeout_ms: config.task_timeout_ms}
         )}
    end
  end

  # Process a full killmail from R2Z2.
  # The killmail already contains full ESI data + zkb metadata - no ESI fetch needed.
  defp process_full_killmail(body) do
    cutoff = get_cutoff_time()

    # The body from R2Z2 already has the zkb block nested.
    # Pass it directly to UnifiedProcessor which handles full killmails.
    case UnifiedProcessor.process_killmail(body, cutoff) do
      {:ok, :kill_older} ->
        {:ok, :kill_older}

      {:ok, enriched_killmail} ->
        broadcast_killmail_update_enriched(enriched_killmail)
        {:ok, :kill_received}

      {:error, reason} ->
        Logger.error("[R2Z2] Failed to process killmail: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Update sequence ID based on poll result
  defp update_sequence_id({:ok, _status, next_sequence_id}, _old_sequence_id)
       when is_integer(next_sequence_id) do
    # Advance to the next sequence on any successful parse (received, older, or skipped)
    next_sequence_id + 1
  end

  defp update_sequence_id(_result, old_sequence_id) do
    # On error or no_kills (404), keep the same sequence ID
    old_sequence_id
  end

  # Decide the next polling interval and updated backoff based on the last result.
  # Returns: {next_delay_ms, updated_backoff_ms}
  defp next_schedule({:ok, kind, _seq}, _old_backoff, config)
       when kind in [:kill_received, :kill_older, :kill_skipped] do
    fast = config.poll_interval_ms

    Logger.debug("[R2Z2] #{kind} → scheduling next poll in #{fast}ms; resetting backoff.")

    {fast, config.initial_backoff_ms}
  end

  defp next_schedule({:ok, :no_kills}, _old_backoff, config) do
    idle = config.idle_interval_ms
    Logger.debug("[R2Z2] No kills (404) → scheduling next poll in #{idle}ms; resetting backoff.")
    {idle, config.initial_backoff_ms}
  end

  defp next_schedule(
         {:error, %Error{type: :rate_limited, details: %{retry_after_ms: ms}}},
         _old_backoff,
         config
       )
       when is_integer(ms) and ms > 0 do
    Logger.warning("[R2Z2] Rate limited, respecting Retry-After: #{ms}ms")
    {ms, config.initial_backoff_ms}
  end

  defp next_schedule({:error, reason}, old_backoff, config) do
    factor = config.backoff_factor
    max_back = config.max_backoff_ms
    next_back = min(old_backoff * factor, max_back)

    Logger.warning(
      "[R2Z2] Poll error, retrying",
      error_type: error_type(reason),
      backoff_ms: next_back,
      max_backoff_ms: max_back,
      reason: String.slice(format_error(reason), 0, 512)
    )

    {next_back, next_back}
  end

  defp error_type(%Error{type: type}), do: type
  defp error_type(_), do: :unknown

  defp format_error(%Error{} = error) do
    "#{error.domain}: #{error.type} - #{error.message}"
  end

  defp format_error(reason) do
    inspect(reason)
  end

  # Updates statistics based on poll result
  defp update_stats(stats, {:ok, :kill_received, _seq}) do
    %{
      stats
      | killmails_received: stats.killmails_received + 1,
        total_killmails_received: stats.total_killmails_received + 1,
        last_killmail_received_at: System.system_time(:second)
    }
  end

  defp update_stats(stats, {:ok, :kill_older, _seq}) do
    %{
      stats
      | killmails_older: stats.killmails_older + 1,
        total_killmails_older: stats.total_killmails_older + 1
    }
  end

  defp update_stats(stats, {:ok, :kill_skipped, _seq}) do
    %{
      stats
      | killmails_skipped: stats.killmails_skipped + 1,
        total_killmails_skipped: stats.total_killmails_skipped + 1
    }
  end

  defp update_stats(stats, {:ok, :no_kills}) do
    %{
      stats
      | no_kills_count: stats.no_kills_count + 1,
        total_no_kills_count: stats.total_no_kills_count + 1
    }
  end

  defp update_stats(stats, {:error, _reason}) do
    %{stats | errors: stats.errors + 1, total_errors: stats.total_errors + 1}
  end

  # Track active systems
  defp track_system_activity(stats, system_id) when is_integer(system_id) do
    %{stats | systems_active: MapSet.put(stats.systems_active, system_id)}
  end

  defp track_system_activity(stats, _), do: stats

  # Update ETS with current stats for real-time dashboard access
  defp update_ets_stats(stats) do
    if :ets.info(EtsOwner.wanderer_kills_stats_table()) != :undefined do
      :ets.insert(EtsOwner.wanderer_kills_stats_table(), {:r2z2_stats, stats})
    end
  end

  # Update circuit breaker status in ETS for non-blocking access
  defp update_circuit_status_ets(circuit_state, consecutive_errors, circuit_opened_at, config) do
    if :ets.info(EtsOwner.wanderer_kills_stats_table()) != :undefined do
      status = %{
        circuit_state: circuit_state,
        consecutive_errors: consecutive_errors,
        max_consecutive_errors: config.max_consecutive_errors,
        circuit_opened_at: circuit_opened_at,
        circuit_reset_timeout_ms: config.circuit_reset_timeout_ms,
        last_updated: System.system_time(:millisecond)
      }

      :ets.insert(EtsOwner.wanderer_kills_stats_table(), {:r2z2_circuit_status, status})
    end
  end

  # Log summary of activity over the past minute
  defp log_summary(stats) do
    duration = DateTime.diff(DateTime.utc_now(), stats.last_reset, :second)

    # Store stats in ETS for unified status reporter
    update_ets_stats(stats)

    # Only log if there's significant error activity
    if stats.errors > 10 do
      Logger.warning(
        "[R2Z2] High error rate detected",
        r2z2_errors: stats.errors,
        r2z2_duration_s: duration
      )
    end
  end

  # Returns cutoff DateTime (e.g. "1 hour ago")
  defp get_cutoff_time do
    Utils.hours_ago(1)
  end

  # Broadcast killmail update to PubSub subscribers using enriched killmail
  defp broadcast_killmail_update_enriched(%Killmail{} = killmail) do
    system_id = killmail.system_id

    Logger.info("[R2Z2] Broadcasting kill #{killmail.killmail_id} to system #{system_id}")

    # Track system activity for statistics
    send(self(), {:track_system, system_id})

    # Broadcast detailed kill update - convert to map for compatibility
    killmail_map = Killmail.to_map(killmail)

    # Send to subscription workers
    SubscriptionManager.broadcast_killmail_update_async(system_id, [
      killmail_map
    ])

    # Also broadcast to PubSub topics for SSE and WebSocket channels
    Broadcaster.broadcast_killmail_update(system_id, [killmail_map])

    # Also broadcast kill count update (increment by 1)
    SubscriptionManager.broadcast_killmail_count_update_async(system_id, 1)
  end

  # Circuit breaker implementation
  defp check_circuit_breaker(
         %State{circuit_state: :open, circuit_opened_at: opened_at} = state,
         config
       ) do
    # Check if enough time has passed to attempt reset
    elapsed_ms = System.monotonic_time(:millisecond) - opened_at

    if elapsed_ms >= config.circuit_reset_timeout_ms do
      Logger.info(
        "[R2Z2] Circuit breaker timeout expired - attempting to reset to half-open state"
      )

      {:ok, %State{state | circuit_state: :half_open}}
    else
      {:circuit_open, state}
    end
  end

  defp check_circuit_breaker(state, _config) do
    {:ok, state}
  end

  # Explicitly handle half-open failure: re-open immediately
  defp update_circuit_state(
         {:error, _reason},
         consecutive_errors,
         :half_open,
         _opened_at,
         _config
       ) do
    Logger.error("[R2Z2] Circuit breaker re-opening after failure in half-open state")
    {consecutive_errors + 1, :open, System.monotonic_time(:millisecond)}
  end

  defp update_circuit_state(
         {:error, _reason},
         consecutive_errors,
         circuit_state,
         circuit_opened_at,
         config
       ) do
    new_consecutive_errors = consecutive_errors + 1

    if new_consecutive_errors >= config.max_consecutive_errors do
      case circuit_state do
        :open ->
          # Circuit already open, keep existing timestamp
          {new_consecutive_errors, :open, circuit_opened_at}

        _ ->
          # Transitioning to open state, set new timestamp
          Logger.error(
            "[R2Z2] Circuit breaker opening after #{new_consecutive_errors} consecutive errors"
          )

          {new_consecutive_errors, :open, System.monotonic_time(:millisecond)}
      end
    else
      {new_consecutive_errors, :closed, circuit_opened_at}
    end
  end

  defp update_circuit_state(
         result,
         _consecutive_errors,
         circuit_state,
         _circuit_opened_at,
         _config
       )
       when is_tuple(result) and elem(result, 0) == :ok do
    reset_circuit_on_success(circuit_state)
  end

  defp reset_circuit_on_success(circuit_state) do
    if circuit_state == :half_open do
      Logger.info("[R2Z2] Circuit breaker reset to closed state after successful request")
    end

    {0, :closed, nil}
  end
end
