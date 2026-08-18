// Google Analytics 4 — wzorzec opt-in przeniesiony z un-hrdb (generalcomments):
// GA ładuje się DOPIERO po zgodzie z bannera; wybór trwały w localStorage.
// Ruch z automatyzacji (webdriver/headless), localhost, ?notrack=1 i Do-Not-Track
// nigdy nie ładuje GA i nie widzi bannera.
(function () {
  // Wstaw identyfikator pomiaru GA4 (Administracja → Strumienie danych → G-XXXXXXXXXX).
  // Pusty = cała analityka wyłączona (bez bannera, bez requestów).
  var GA_ID = "";
  var CONSENT_KEY = "tk_ga_consent_v1";
  var gaLoaded = false;

  function suppressed() {
    try {
      if (navigator.webdriver) return true;
      if (/HeadlessChrome/.test(navigator.userAgent || "")) return true;
      var h = location.hostname;
      if (h === "localhost" || h === "127.0.0.1" || h === "") return true;
      if (new URLSearchParams(location.search).get("notrack") === "1") return true;
      if (navigator.doNotTrack === "1" || window.doNotTrack === "1") return true;
    } catch (e) { /* w razie wątpliwości nie tłumimy */ }
    return false;
  }

  function loadAnalytics() {
    if (gaLoaded || typeof window.gtag === "function") return;
    if (!GA_ID || suppressed()) return;
    gaLoaded = true;
    window.dataLayer = window.dataLayer || [];
    window.gtag = function gtag() { window.dataLayer.push(arguments); };
    window.gtag("js", new Date());
    window.gtag("config", GA_ID, { anonymize_ip: true });
    var s = document.createElement("script");
    s.async = true;
    s.src = "https://www.googletagmanager.com/gtag/js?id=" + GA_ID;
    document.head.appendChild(s);
  }

  function track(eventName, params) {
    if (typeof window.gtag === "function") window.gtag("event", eventName, params || {});
  }

  function initConsent() {
    if (!GA_ID || suppressed()) return;
    var choice = null;
    try { choice = localStorage.getItem(CONSENT_KEY); } catch (e) { /* np. tryb prywatny */ }
    if (choice === "granted") { loadAnalytics(); return; }
    if (choice === "denied") return;
    var banner = document.getElementById("consentBanner");
    if (!banner) return;
    banner.hidden = false;
    var decide = function (val) {
      try { localStorage.setItem(CONSENT_KEY, val); } catch (e) { /* ignoruj */ }
      banner.hidden = true;
    };
    var accept = document.getElementById("consentAccept");
    var decline = document.getElementById("consentDecline");
    if (accept) accept.addEventListener("click", function () { decide("granted"); loadAnalytics(); });
    if (decline) decline.addEventListener("click", function () { decide("denied"); });
  }

  // Zdarzenia kluczowych akcji — delegacja, bez ingerencji w kod aplikacji.
  // Słownik zdarzeń rekomendowanych GA4 (search, select_content), więc trafiają
  // do standardowych raportów bez własnych wymiarów.
  function initEvents() {
    var searchForm = document.getElementById("searchForm");
    if (searchForm) {
      searchForm.addEventListener("submit", function () {
        var input = document.getElementById("searchInput");
        var term = input && input.value ? String(input.value).trim().slice(0, 100) : "";
        if (term) track("search", { search_term: term });
      });
    }
    document.addEventListener("click", function (event) {
      var target = event.target instanceof Element ? event.target : null;
      if (!target) return;
      var openCase = target.closest("[data-action='open-case-view']");
      if (openCase) {
        track("select_content", { content_type: "judgment" });
        return;
      }
      if (target.closest("#themeToggle")) {
        track("theme_toggle", {});
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    initConsent();
    initEvents();
  });
})();
