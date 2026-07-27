// CORS-injecting Worker for the parliamentwatch-data Workers Static Assets
// project. Fetches from the ASSETS binding (./docs), then attaches CORS
// headers to every response so the SansadSaar app (different origin) can
// fetch from this mirror.
//
// Why this exists instead of relying on `_headers`:
// CF Workers Static Assets does NOT process the `_headers` file the way
// CF Pages does (or the way the legacy GitHub-integrated CF Workers
// build did). When we migrated to wrangler-based cf-sync deploys, the
// `_headers` file got deployed AS A STATIC FILE rather than as routing
// config — so every cross-origin fetch from sansadsaar.naklitechie.com
// to *.naklitechie.com/{corpus}/meta.json started failing with the
// browser's "Failed to fetch" CORS block. Incognito users hit it first
// (no IDB cache to fall back on).
//
// Diagnosed 2026-05-16 11:45 UTC; the fix is this single Worker
// wrapping the assets handler.

export default {
  async fetch(request, env, ctx) {
    // Cheap preflight handler — never reach the assets fetch for OPTIONS.
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
          "Access-Control-Allow-Headers": "*",
          "Access-Control-Max-Age": "86400",
          "Vary": "Origin",
        },
      });
    }

    // Delegate to the assets server. The ASSETS binding is configured
    // in wrangler.toml as `[assets] binding = "ASSETS"`.
    const response = await env.ASSETS.fetch(request);

    // Clone response so we can mutate headers (CF responses are immutable
    // by default).
    const headers = new Headers(response.headers);
    headers.set("Access-Control-Allow-Origin", "*");
    headers.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    headers.set("Vary", "Origin");

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  },
};
