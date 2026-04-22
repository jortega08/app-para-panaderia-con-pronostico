(function () {
  const meta = document.querySelector('meta[name="csrf-token"]');
  const csrfToken = meta ? meta.getAttribute("content") : "";
  const safeMethods = new Set(["GET", "HEAD", "OPTIONS", "TRACE"]);

  if (csrfToken && typeof window.fetch === "function") {
    const nativeFetch = window.fetch.bind(window);
    window.fetch = function patchedFetch(input, init) {
      const options = init ? { ...init } : {};
      const method = String(options.method || "GET").toUpperCase();
      const url = typeof input === "string" ? input : (input && input.url) || "";
      const isSameOrigin = !url || url.startsWith("/") || url.startsWith(window.location.origin);

      if (isSameOrigin && !safeMethods.has(method)) {
        const headers = new Headers(options.headers || (input && input.headers) || {});
        if (!headers.has("X-CSRF-Token")) {
          headers.set("X-CSRF-Token", csrfToken);
        }
        options.headers = headers;
      }
      return nativeFetch(input, options);
    };
  }

  document.addEventListener("submit", function (event) {
    const form = event.target;
    if (!form || form.tagName !== "FORM" || !csrfToken) return;
    const method = String(form.getAttribute("method") || "GET").toUpperCase();
    if (safeMethods.has(method)) return;
    if (form.querySelector('input[name="_csrf_token"]')) return;

    const input = document.createElement("input");
    input.type = "hidden";
    input.name = "_csrf_token";
    input.value = csrfToken;
    form.appendChild(input);
  }, true);
})();
