// FUSE API — cliente LLM compartido para los demos
const FUSE_API = {
  // Auto-detect base URL: if served from /fuse-demo/, use same origin. If local, hardcode.
  baseUrl: (window.location.origin.includes("localhost") || window.location.protocol === "file:")
    ? "https://rodmatdashboard-production.up.railway.app"
    : window.location.origin,
  demoToken: "fuse-demo-2026",

  async chat(messages, opts = {}) {
    const body = {
      messages,
      temperature: opts.temperature ?? 0.4,
      max_tokens: opts.max_tokens ?? 1200,
      response_format: opts.jsonMode ? "json_object" : null,
    };
    try {
      const r = await fetch(`${this.baseUrl}/api/fuse-demo/chat`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Demo-Token": this.demoToken,
        },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const err = await r.text().catch(() => "");
        throw new Error(`API ${r.status}: ${err.slice(0, 200)}`);
      }
      const data = await r.json();
      return data.content;
    } catch (e) {
      console.error("FUSE API error:", e);
      throw e;
    }
  },
};

// Utility: safe JSON extraction from LLM response (handles ```json blocks and text noise)
function extractJSON(text) {
  const match = text.match(/```json\s*([\s\S]*?)```/) || text.match(/\{[\s\S]*\}/);
  const raw = match ? (match[1] || match[0]) : text;
  try { return JSON.parse(raw); } catch { return null; }
}
