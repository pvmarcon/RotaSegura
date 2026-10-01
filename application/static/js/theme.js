(() => {
  const storageKey = "rotasegura-theme";
  const root = document.documentElement;
  const savedTheme = localStorage.getItem(storageKey);
  if (savedTheme === "light") root.classList.add("light-theme");

  function updateToggle(button) {
    const isLight = root.classList.contains("light-theme");
    button.setAttribute("aria-pressed", String(isLight));
    button.setAttribute("aria-label", isLight ? "Ativar modo escuro" : "Ativar modo claro");
    button.title = isLight ? "Ativar modo escuro" : "Ativar modo claro";
    button.querySelector(".theme-toggle-label").textContent = isLight ? "Escuro" : "Claro";
    button.querySelector(".theme-toggle-icon").textContent = isLight ? "☾" : "☀";
  }

  function setupToggle() {
    document.querySelectorAll("[data-theme-toggle]").forEach((button) => {
      updateToggle(button);
      button.addEventListener("click", () => {
        root.classList.toggle("light-theme");
        localStorage.setItem(storageKey, root.classList.contains("light-theme") ? "light" : "dark");
        document.querySelectorAll("[data-theme-toggle]").forEach(updateToggle);
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", setupToggle);
  } else {
    setupToggle();
  }
})();
