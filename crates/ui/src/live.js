// Keep the page current without a framework: re-fetch it and swap the
// regions marked data-live. Forms live outside those regions, so typing is
// never interrupted.
(function () {
  // Flash messages are for this view only: drop them from the address.
  var url = new URL(location.href);
  if (url.searchParams.has("ok") || url.searchParams.has("err")) {
    url.searchParams.delete("ok");
    url.searchParams.delete("err");
    history.replaceState(null, "", url);
  }

  var busy = false;
  async function refresh() {
    if (busy || document.hidden || !document.querySelector("[data-live]")) return;
    busy = true;
    try {
      var response = await fetch(location.href, { headers: { "x-wfe-live": "1" } });
      var doc = new DOMParser().parseFromString(await response.text(), "text/html");
      document.querySelectorAll("[data-live]").forEach(function (el) {
        var fresh = doc.querySelector('[data-live="' + el.dataset.live + '"]');
        if (fresh && fresh.innerHTML !== el.innerHTML && !el.contains(document.activeElement)) {
          el.innerHTML = fresh.innerHTML;
        }
      });
      document.body.classList.remove("stale");
    } catch (e) {
      document.body.classList.add("stale");
    }
    busy = false;
  }
  setInterval(refresh, 2000);

  document.addEventListener("submit", function (event) {
    var message = event.target.dataset && event.target.dataset.confirm;
    if (message && !confirm(message)) event.preventDefault();
  });

  // One tooltip for anything with data-tip.
  var tip = document.getElementById("tip");
  document.addEventListener("mouseover", function (event) {
    var target = event.target.closest && event.target.closest("[data-tip]");
    if (!target) {
      tip.hidden = true;
      return;
    }
    tip.textContent = target.dataset.tip;
    tip.hidden = false;
    var box = target.getBoundingClientRect();
    var left = Math.min(Math.max(8, box.left + box.width / 2 - tip.offsetWidth / 2), innerWidth - tip.offsetWidth - 8);
    var top = box.top - tip.offsetHeight - 8;
    tip.style.left = left + "px";
    tip.style.top = (top < 8 ? box.bottom + 8 : top) + "px";
  });
  document.addEventListener("scroll", function () { tip.hidden = true; }, true);
})();
