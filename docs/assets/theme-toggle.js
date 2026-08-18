// Przełącznik trybu ciemnego — stosowany przed pierwszym malowaniem
// (skrypt ładowany synchronicznie w <head>).
(function () {
  var KEY = "tk-theme";
  var stored = null;
  try { stored = localStorage.getItem(KEY); } catch (e) { /* np. tryb prywatny */ }
  var theme = stored === "dark" || stored === "light"
    ? stored
    : (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  document.documentElement.setAttribute("data-theme", theme);

  document.addEventListener("DOMContentLoaded", function () {
    var button = document.getElementById("themeToggle");
    if (!button) return;
    button.addEventListener("click", function () {
      var next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
      document.documentElement.setAttribute("data-theme", next);
      try { localStorage.setItem(KEY, next); } catch (e) { /* ignoruj */ }
    });
  });
})();
