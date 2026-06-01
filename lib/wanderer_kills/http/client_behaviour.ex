defmodule WandererKills.Http.ClientBehaviour do
  @moduledoc """
  Behaviour definition for HTTP client implementations.

  This allows for easy mocking in tests and potential alternative implementations.
  """

  @type url :: String.t()
  @type headers :: [{String.t(), String.t()}]
  @type request_option ::
          {:timeout, non_neg_integer()}
          | {:params, map() | keyword()}
          | {atom(), term()}
  @type options :: keyword()
  @type response :: {:ok, map()} | {:error, term()}

  @doc """
  Performs a GET request.

  Implementations should support shared options such as:
  - `:timeout` - request timeout in milliseconds
  - `:params` - query params appended to the URL
  """
  @callback get(url, headers, [request_option()]) :: response
  @callback get_with_rate_limit(url, headers, options) :: response
  @callback post(url, body :: term(), headers, options) :: response
  @callback get_esi(url, headers, options) :: response
  @callback get_zkb(url, headers, options) :: response
  @callback get_r2z2(url, headers, options) :: response
end
