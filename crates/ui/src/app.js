(() => {
  "use strict";

  const root = document.documentElement;
  const $ = (selector, scope) => (scope || document).querySelector(selector);
  const $$ = (selector, scope) => Array.from((scope || document).querySelectorAll(selector));
  const content = () => $("#content");
  const parse = (html) => new DOMParser().parseFromString(html, "text/html");
  const UNREACHABLE = "Can't reach the UI server. Is wfe ui still running?";

  const toasts = $("#toasts");
  function dismiss(el) {
    if (el.classList.contains("out")) return;
    el.classList.add("out");
    setTimeout(() => el.remove(), 220);
  }
  function arm(el) {
    let timer = setTimeout(() => dismiss(el), el.classList.contains("bad") ? 9000 : 4000);
    el.addEventListener("mouseenter", () => clearTimeout(timer));
    el.addEventListener("mouseleave", () => { timer = setTimeout(() => dismiss(el), 1500); });
  }
  function toast(kind, message) {
    const el = document.createElement("div");
    el.className = "toast " + kind;
    el.setAttribute("role", kind === "bad" ? "alert" : "status");
    const text = document.createElement("span");
    text.textContent = message;
    const close = document.createElement("button");
    close.className = "toast-x";
    close.type = "button";
    close.setAttribute("aria-label", "Dismiss");
    close.textContent = "×";
    el.append(text, close);
    toasts.append(el);
    while (toasts.children.length > 4) toasts.firstElementChild.remove();
    arm(el);
  }

  function takeFlash(href) {
    const url = new URL(href, location.href);
    const flash = { url: url, ok: url.searchParams.get("ok"), err: url.searchParams.get("err") };
    url.searchParams.delete("ok");
    url.searchParams.delete("err");
    return flash;
  }
  const landed = takeFlash(location.href);
  if (landed.ok || landed.err) history.replaceState(history.state, "", landed.url);
  $$(".toast").forEach(arm);

  const bar = $("#progress");
  let barTimer;
  const progress = {
    start() {
      clearTimeout(barTimer);
      bar.className = "progress";
      void bar.offsetWidth;
      bar.className = "progress run";
    },
    done() {
      bar.className = "progress done";
      barTimer = setTimeout(() => { bar.className = "progress"; }, 450);
    },
  };

  function keeps(el, name) {
    if (name === "open") return el.tagName === "DETAILS" || el.tagName === "DIALOG";
    if (name === "style") return el.classList.contains("menu-list");
    return false;
  }
  function morph(from, to) {
    if (from.nodeType !== 1) {
      if (from.nodeValue !== to.nodeValue) from.nodeValue = to.nodeValue;
      return;
    }
    if (from.tagName === "DIALOG" && from.open) return;
    for (const attr of Array.from(from.attributes)) {
      if (!to.hasAttribute(attr.name) && !keeps(from, attr.name)) from.removeAttribute(attr.name);
    }
    for (const attr of Array.from(to.attributes)) {
      if (from.getAttribute(attr.name) !== attr.value && !keeps(from, attr.name)) from.setAttribute(attr.name, attr.value);
    }
    if (from === document.activeElement && /^(INPUT|TEXTAREA|SELECT)$/.test(from.tagName)) return;
    const old = Array.from(from.childNodes);
    const fresh = Array.from(to.childNodes);
    fresh.forEach((next, i) => {
      const current = old[i];
      if (!current) from.appendChild(document.importNode(next, true));
      else if (current.nodeType !== next.nodeType || current.nodeName !== next.nodeName) from.replaceChild(document.importNode(next, true), current);
      else morph(current, next);
    });
    for (let i = fresh.length; i < old.length; i++) old[i].remove();
  }

  function rememberCell() {
    const cell = $("#side").dataset.cell;
    if (cell) document.cookie = "wfe_cell=" + encodeURIComponent(cell) + "; path=/; max-age=31536000; samesite=lax";
  }
  rememberCell();

  function apply(doc, url, options) {
    document.title = doc.title;
    morph($("#side"), $("#side", doc));
    morph($("#bar"), $("#bar", doc));
    const next = $("#content", doc);
    if (options.fresh) content().replaceWith(document.importNode(next, true));
    else morph(content(), next);
    if (options.push) history.pushState({}, "", url);
    else history.replaceState(history.state, "", url);
    document.body.classList.remove("side-open", "offline");
    rememberCell();
  }

  function showSkeleton() {
    if ($("#loading")) return;
    const el = document.createElement("main");
    el.id = "loading";
    el.className = "skeleton";
    el.append($("#skeleton").content.cloneNode(true));
    el.style.cssText = "width:100%;max-width:1240px;margin:0 auto;padding:28px 28px 80px";
    content().hidden = true;
    content().after(el);
  }
  function hideSkeleton() {
    const el = $("#loading");
    if (el) el.remove();
    content().hidden = false;
  }

  let visits = 0;
  let submitting = 0;
  const scrolls = new Map();
  history.scrollRestoration = "manual";

  async function visit(href, options) {
    const push = !options || options.push !== false;
    const mine = ++visits;
    scrolls.set(location.href, scrollY);
    progress.start();
    const slow = setTimeout(() => { if (mine === visits) showSkeleton(); }, 220);
    try {
      const response = await fetch(href, { headers: { "x-wfe": "nav" } });
      const doc = parse(await response.text());
      if (mine !== visits) return;
      if (!$("#content", doc)) {
        location.assign(href);
        return;
      }
      hideSkeleton();
      const flash = takeFlash(response.url);
      apply(doc, flash.url, { push: push, fresh: true });
      scrollTo(0, push ? 0 : scrolls.get(location.href) || 0);
      if (flash.ok) toast("good", flash.ok);
      if (flash.err) toast("bad", flash.err);
    } catch (error) {
      if (mine !== visits) return;
      hideSkeleton();
      toast("bad", UNREACHABLE);
    } finally {
      clearTimeout(slow);
      if (mine === visits) progress.done();
    }
  }
  addEventListener("popstate", () => visit(location.href, { push: false }));

  function busy(button, on) {
    if (!button) return;
    if (on) button.dataset.busy = "";
    else delete button.dataset.busy;
    button.disabled = on;
  }
  function dialogError(dialog, message) {
    const box = $(".dialog-error", dialog);
    if (!box) return toast("bad", message);
    box.textContent = message;
    box.hidden = true;
    void box.offsetWidth;
    box.hidden = false;
  }

  async function send(form, submitter) {
    const data = new URLSearchParams(new FormData(form));
    if (submitter && submitter.name) data.append(submitter.name, submitter.value);
    const dialog = form.closest("dialog");
    const button = submitter || $("button[type=submit]", form);
    busy(button, true);
    submitting++;
    progress.start();
    try {
      const response = await fetch(form.getAttribute("action"), { method: "POST", body: data, headers: { "x-wfe": "form" } });
      const text = await response.text();
      const doc = parse(text);
      const flash = takeFlash(response.url);
      if (flash.err || !$("#content", doc)) {
        const message = flash.err || text.trim().slice(0, 240) || "The request failed.";
        if (dialog && dialog.open) dialogError(dialog, message);
        else toast("bad", message);
        return;
      }
      if (dialog && dialog.open) dialog.close();
      form.reset();
      const same = flash.url.pathname + flash.url.search === location.pathname + location.search;
      visits++;
      hideSkeleton();
      apply(doc, flash.url, { push: !same, fresh: !same });
      if (!same) scrollTo(0, 0);
      if (flash.ok) toast("good", flash.ok);
    } catch (error) {
      if (dialog && dialog.open) dialogError(dialog, UNREACHABLE);
      else toast("bad", UNREACHABLE);
    } finally {
      submitting--;
      busy(button, false);
      progress.done();
    }
  }

  const confirmBox = $("#confirm");
  const confirmOk = $("#confirm-ok");
  let asking = null;
  function ask(form, submitter) {
    asking = { form: form, submitter: submitter };
    $("#confirm-title").textContent = form.dataset.confirm;
    const body = $("#confirm-body");
    body.textContent = form.dataset.confirmBody || "";
    body.hidden = !form.dataset.confirmBody;
    confirmOk.textContent = form.dataset.confirmCta || "Confirm";
    confirmOk.className = "btn " + (form.hasAttribute("data-danger") ? "danger solid" : "primary");
    confirmBox.showModal();
  }
  confirmOk.addEventListener("click", async () => {
    const pending = asking;
    if (!pending) return;
    asking = null;
    busy(confirmOk, true);
    await send(pending.form, pending.submitter);
    busy(confirmOk, false);
    confirmBox.close();
  });

  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (form.hasAttribute("data-native")) return;
    event.preventDefault();
    if ((form.getAttribute("method") || "get").toLowerCase() === "get") {
      visit(queryUrl(form));
      return;
    }
    if (form.dataset.confirm) ask(form, event.submitter);
    else send(form, event.submitter);
  });

  function queryUrl(form) {
    const url = new URL(form.getAttribute("action") || location.pathname, location.href);
    const params = new URLSearchParams();
    new FormData(form).forEach((value, key) => { if (value !== "") params.append(key, value); });
    url.search = params.toString();
    return url;
  }

  function openDialog(opener) {
    const dialog = document.getElementById(opener.dataset.open);
    if (!dialog) return;
    const error = $(".dialog-error", dialog);
    if (error) error.hidden = true;
    const fill = opener.dataset.fill ? JSON.parse(opener.dataset.fill) : {};
    Object.keys(fill).forEach((key) => {
      $$('[name="' + key + '"]', dialog).forEach((el) => {
        if (el.type === "radio") el.checked = el.value === String(fill[key]);
        else el.value = fill[key];
        el.dispatchEvent(new Event("input", { bubbles: true }));
      });
      $$('[data-slot="' + key + '"]', dialog).forEach((el) => { el.textContent = fill[key]; });
    });
    $$("form", dialog).forEach(syncForm);
    dialog.showModal();
  }

  function syncForm(form) {
    $$("[data-show-when]", form).forEach((el) => {
      const pair = el.dataset.showWhen.split("=");
      el.hidden = new FormData(form).get(pair[0]) !== pair[1];
    });
    $$("input[type=range]", form).forEach(syncRange);
  }
  function syncRange(input) {
    input.style.setProperty("--p", input.value + "%");
    const output = input.parentElement.querySelector("output");
    if (output) output.textContent = input.value + "%";
  }

  let typing;
  document.addEventListener("input", (event) => {
    const target = event.target;
    if (!target.form) return;
    syncForm(target.form);
    if (target.form.hasAttribute("data-auto")) {
      clearTimeout(typing);
      typing = setTimeout(() => {
        history.replaceState(history.state, "", queryUrl(target.form));
        refresh(true);
      }, 200);
    }
  });

  function closeMenus(except) {
    $$("details.menu[open]").forEach((menu) => { if (menu !== except) menu.open = false; });
  }
  document.addEventListener("toggle", (event) => {
    const menu = event.target;
    if (!menu.matches || !menu.matches("details.menu") || !menu.open) return;
    closeMenus(menu);
    const list = $(".menu-list", menu);
    const anchor = $("summary", menu).getBoundingClientRect();
    const below = anchor.bottom + 6;
    if (menu.classList.contains("switcher")) list.style.minWidth = anchor.width + "px";
    list.style.left = (menu.classList.contains("switcher") ? anchor.left : Math.max(8, anchor.right - list.offsetWidth)) + "px";
    list.style.top = (below + list.offsetHeight > innerHeight - 8 ? anchor.top - list.offsetHeight - 6 : below) + "px";
  }, true);
  addEventListener("resize", () => closeMenus());

  async function copy(button) {
    const text = button.dataset.copy;
    try {
      await navigator.clipboard.writeText(text);
    } catch (error) {
      const area = document.createElement("textarea");
      area.value = text;
      area.style.cssText = "position:fixed;opacity:0";
      document.body.append(area);
      area.select();
      document.execCommand("copy");
      area.remove();
    }
    button.classList.add("copied");
    setTimeout(() => button.classList.remove("copied"), 1400);
  }

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    hideTip();

    const openMenu = target.closest("details.menu");
    if (!openMenu) closeMenus();
    else if (target.closest(".menu-item")) setTimeout(closeMenus, 0);

    if (target.tagName === "DIALOG") {
      target.close();
      return;
    }
    const closer = target.closest("[data-close]");
    if (closer) {
      closer.closest("dialog").close();
      return;
    }
    const opener = target.closest("[data-open]");
    if (opener) {
      openDialog(opener);
      return;
    }
    const copier = target.closest("[data-copy]");
    if (copier) {
      event.preventDefault();
      event.stopPropagation();
      copy(copier);
      return;
    }
    if (target.closest("[data-theme-toggle]")) {
      const next = root.dataset.theme === "dark" ? "light" : "dark";
      root.classList.add("theming");
      root.dataset.theme = next;
      try { localStorage.setItem("wfe-theme", next); } catch (error) { void error; }
      setTimeout(() => root.classList.remove("theming"), 300);
      return;
    }
    if (target.closest("[data-side-toggle]")) {
      document.body.classList.toggle("side-open");
      return;
    }
    if (document.body.classList.contains("side-open") && !target.closest("#side")) document.body.classList.remove("side-open");
    if (target.closest(".toast-x")) {
      dismiss(target.closest(".toast"));
      return;
    }

    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    const link = target.closest("a[href]");
    if (link) {
      if (link.target || link.hasAttribute("download") || link.origin !== location.origin || link.pathname.startsWith("/assets/")) return;
      event.preventDefault();
      visit(link.href);
      return;
    }
    const row = target.closest("tr[data-href]");
    if (row && !target.closest("button, input, select, summary, form") && String(getSelection()) === "") visit(row.dataset.href);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeMenus();
    if (event.key === "/" && !/^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName) && !$("dialog[open]")) {
      const search = $("[data-search]");
      if (search) {
        event.preventDefault();
        search.focus();
        search.select();
      }
    }
  });

  let refreshing = false;
  let again = false;
  async function refresh(force) {
    if (refreshing) {
      again = again || force;
      return;
    }
    if (submitting || document.hidden || $("#loading") || !(force || content().hasAttribute("data-live"))) return;
    refreshing = true;
    const mine = visits;
    const href = location.href;
    try {
      const response = await fetch(href, { headers: { "x-wfe": "live" } });
      const doc = parse(await response.text());
      const next = $("#content", doc);
      if (next && mine === visits && href === location.href && !submitting) {
        morph(content(), next);
        morph($("#side"), $("#side", doc));
      }
      document.body.classList.remove("offline");
    } catch (error) {
      document.body.classList.add("offline");
    }
    refreshing = false;
    if (again) {
      again = false;
      refresh(true);
    }
  }
  setInterval(refresh, 2000);
  document.addEventListener("visibilitychange", () => refresh(false));

  const tip = $("#tip");
  function hideTip() {
    tip.hidden = true;
  }
  document.addEventListener("mouseover", (event) => {
    const target = event.target instanceof Element && event.target.closest("[data-tip]");
    if (!target) return hideTip();
    if (tip.textContent !== target.dataset.tip || tip.hidden) {
      tip.hidden = true;
      tip.textContent = target.dataset.tip;
      tip.hidden = false;
    }
    const box = target.getBoundingClientRect();
    const left = Math.min(Math.max(8, box.left + box.width / 2 - tip.offsetWidth / 2), innerWidth - tip.offsetWidth - 8);
    const top = box.top - tip.offsetHeight - 8;
    tip.style.left = left + "px";
    tip.style.top = (top < 8 ? box.bottom + 8 : top) + "px";
  });
  document.addEventListener("scroll", () => {
    hideTip();
    closeMenus();
  }, true);

  $$("form").forEach(syncForm);
})();
