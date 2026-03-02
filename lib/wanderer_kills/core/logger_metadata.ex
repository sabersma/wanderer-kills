defmodule WandererKills.Core.LoggerMetadata do
  @moduledoc """
  Logger metadata configuration for the WandererKills application.

  This module defines all metadata fields that can be included in log messages
  throughout the application, organized by domain.
  """

  # Standard Elixir metadata
  @standard_metadata [
    :request_id,
    :application,
    :module,
    :function,
    :line
  ]

  # Core application metadata
  @core_metadata [
    :system_id,
    :killmail_id,
    :operation,
    :step,
    :status,
    :error,
    :duration,
    :source,
    :reason,
    :type,
    :message,
    :stacktrace,
    :timestamp,
    :kind
  ]

  # HTTP and API metadata
  @http_metadata [
    :url,
    :method,
    :service,
    :endpoint,
    :duration_ms,
    :response_size,
    :query_string,
    :remote_ip
  ]

  # EVE Online entity metadata
  @eve_metadata [
    :character_id,
    :corporation_id,
    :alliance_id,
    :type_id,
    :solar_system_id,
    :ship_type_id,
    :victim_character,
    :victim_corp,
    :victim_ship,
    :attacker_count,
    :total_value,
    :npc_kill
  ]

  # Cache metadata
  @cache_metadata [
    :cache,
    :cache_key,
    :cache_type,
    :ttl,
    :namespace,
    :id
  ]

  # Processing metadata
  @processing_metadata [
    :killmail_count,
    :count,
    :result,
    :data_source,
    :payload_size_bytes,
    :fresh_kills_fetched,
    :kills_to_process,
    :updates,
    :killmails_received,
    :killmails_older,
    :killmails_skipped,
    :legacy_kills,
    :no_kills_polls,
    :errors,
    :active_systems,
    :total_polls,
    :requested_count,
    :returned_count,
    :total_killmails,
    :filtered_killmails,
    :returned_killmails,
    :killmail_ids_count,
    :filter_recent,
    :total_cached_killmails,
    :sample_killmail_keys
  ]

  # WebSocket and connection metadata
  @websocket_metadata [
    :systems,
    :active_connections,
    :kills_sent_realtime,
    :kills_sent_preload,
    :kills_per_minute,
    :connections_per_minute,
    :user_id,
    :subscription_id,
    :systems_count,
    :peer_data,
    :user_agent,
    :initial_systems_count,
    :new_systems_count,
    :total_systems_count,
    :removed_systems_count,
    :remaining_systems_count,
    :total_systems,
    :total_kills_sent,
    :limit,
    :total_cached_ids,
    :sample_ids,
    :requested_ids,
    :enriched_found,
    :enriched_missing,
    :failed_samples,
    :killmail_ids,
    :cached_ids_count,
    :subscribed_systems_count,
    :disconnect_reason,
    :connection_duration_seconds,
    :socket_transport,
    # Status report metadata
    :websocket_active_connections,
    :websocket_kills_sent_total,
    :websocket_kills_sent_realtime,
    :websocket_kills_sent_preload,
    :websocket_active_subscriptions,
    :websocket_total_systems,
    :websocket_kills_per_minute,
    :websocket_connections_per_minute,
    :cache_size,
    :store_total_killmails,
    :store_unique_systems
  ]

  # Retry and timeout metadata
  @retry_metadata [
    :attempt,
    :max_attempts,
    :remaining_attempts,
    :delay_ms,
    :timeout,
    :request_type,
    :raw_count,
    :parsed_count,
    :enriched_count,
    :since_hours,
    :provided_id,
    :types,
    :groups,
    :file,
    :path,
    :pass_type,
    :hours,
    :limit,
    :max_concurrency,
    :purpose,
    :format,
    :percentage,
    :description,
    :unit,
    :value,
    :count,
    :total,
    :processed,
    :skipped,
    :error
  ]

  # Analysis metadata
  @analysis_metadata [
    :total_killmails_analyzed,
    :format_distribution,
    :system_distribution,
    :ship_distribution,
    :character_distribution,
    :corporation_distribution,
    :alliance_distribution,
    :ship_type_distribution,
    :purpose,
    :sample_index,
    :sample_size,
    :sample_type,
    :sample_value,
    :sample_unit,
    :sample_structure,
    :data_type,
    :raw_keys,
    :has_full_data,
    :needs_esi_fetch,
    :byte_size,
    :tasks,
    :group_ids,
    :error_count,
    :total_groups,
    :success_count,
    :type_count
  ]

  # Validation metadata
  @validation_metadata [
    :cutoff_time,
    :killmail_sample,
    :required_fields,
    :missing_fields,
    :available_keys,
    :raw_structure,
    :parsed_structure,
    :enriched_structure,
    :killmail_keys,
    :kill_count,
    :hash,
    :has_solar_system_id,
    :has_kill_count,
    :has_hash,
    :has_killmail_id,
    :has_system_id,
    :has_ship_type_id,
    :has_character_id,
    :has_victim,
    :has_attackers,
    :has_zkb,
    :parser_type,
    :killmail_hash,
    :recommendation,
    :structure,
    :kill_time,
    :kill_time_type,
    :kill_time_value,
    :cutoff,
    :has_kill_time,
    :current_time,
    :hours_back,
    :kill_time_string,
    :kill_time_parsed,
    :comparison_result,
    :is_recent,
    :error_type,
    :error_message,
    :has_killmail_time
  ]

  # Subscription metadata
  @subscription_metadata [
    :subscriber_id,
    :system_ids,
    :callback_url,
    :subscription_id,
    :status,
    :system_count,
    :has_callback,
    :total_subscriptions,
    :active_subscriptions,
    :removed_count,
    :requested_systems,
    :successful_systems,
    :failed_systems,
    :total_systems,
    :kills_count,
    :subscriber_count,
    :subscriber_ids,
    :via_pubsub,
    :via_webhook,
    :kills_type,
    :kills_value
  ]

  # PubSub metadata
  @pubsub_metadata [
    :pubsub_name,
    :pubsub_topic,
    :pubsub_message,
    :pubsub_metadata,
    :pubsub_payload,
    :pubsub_headers,
    :pubsub_timestamp,
    :total_kills,
    :filtered_kills,
    :total_cached_kills,
    :cache_error,
    :returned_kills,
    :unexpected_response,
    :cached_count,
    :client_identifier,
    :unenriched_count,
    :kill_time_range
  ]

  # All metadata combined (remove duplicates)
  @all_metadata (@standard_metadata ++
                   @core_metadata ++
                   @http_metadata ++
                   @eve_metadata ++
                   @cache_metadata ++
                   @processing_metadata ++
                   @websocket_metadata ++
                   @retry_metadata ++
                   @analysis_metadata ++
                   @validation_metadata ++
                   @subscription_metadata ++
                   @pubsub_metadata)
                |> Enum.uniq()

  # Development metadata (excludes some verbose fields)
  @dev_metadata ([:request_id, :file, :line] ++
                   @core_metadata ++
                   @http_metadata ++
                   @eve_metadata ++
                   @cache_metadata ++
                   @processing_metadata ++
                   @websocket_metadata ++
                   @retry_metadata ++
                   @analysis_metadata ++
                   @validation_metadata ++
                   @subscription_metadata ++
                   @pubsub_metadata)
                |> Enum.uniq()

  @doc """
  Returns all available metadata fields.
  """
  @spec all() :: [atom()]
  def all, do: @all_metadata

  @doc """
  Returns metadata fields suitable for development environment.

  Excludes some verbose fields like pid, application, and mfa.
  """
  @spec dev() :: [atom()]
  def dev, do: @dev_metadata
end
