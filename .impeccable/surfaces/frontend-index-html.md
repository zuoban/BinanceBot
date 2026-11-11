---
version: 1
slug: "frontend-index-html"
primary_target: "frontend/index.html"
related_targets: []
---

# Surface brief: Grid Console (whole site, Operate)

Scope and mode: Operate. Login, overview, strategy detail, strategy config, global settings, loading states. Desktop-first with full mobile behavior.

Audience / job: Chinese-speaking traders who configure and monitor Binance USDⓈ-M grid strategies, often for long sessions. Task: see account and strategy state at a glance, switch pairs fast, pause/resume safely, edit parameters with confidence.

Constraints: PRODUCT.md truths. Long = green, short = warm red, never colour alone. Stale or uncertain state is always shown in words. Live vs paper always visible. Dirty-form guards, confirmations and all existing behaviour stay.

Chosen direction (seed:184f54d29a5d71e6, assigned candidate 4 of the grounded list: national weather-service warning-signal bulletins)

## Direction contract

THESIS: The console reads like a weather bulletin for a market: calm default, and four graded signal tiers (slate-blue / amber-yellow / warm-orange / alert-red) carry every abnormal condition with the same icon-plus-word grammar. Refuses the category default of a grey dashboard with scattered red/green badges.

OWN-WORLD: Deep charcoal rail (always dark, `#141518` / `#0f1012`) beside a restrained neutral working ground (`#f7f7f8` / `#131416`); high-contrast amber-gold (`#9a6700` / `#e5a910`) for focus/selection/equity trend and crisp ink/light primary buttons; tiered signal colours reserved for state; hairline 1px rules, 8px controls, 12px panels, no shadows except overlays and dirty save bar. Clean sans (`Inter` / `IBM Plex Sans` + CJK) for text, tabular monospace for every figure. Dense bulletin rhythm: figure columns right-aligned, rules between rows instead of boxes inside boxes.

STORY: The visitor sees in one glance whether anything needs attention, then drills to a pair in one click from the rail or ⌘K, and acts with confirmation where money is at risk.

FIRST VIEWPORT: Left rail 248px (brand, ⌘K search, 总览, 设置, live pair list with status dots and direction arrows, mode chip, theme and logout at the foot). Top of content: page title (28px, 600 weight) with the signal strip directly under it (tiered chips for 数据过期, 实盘策略, 异常 n, 已暂停 n, 已停止 n; calm state shows a single quiet 全部策略运行正常 chip). Then a four-figure summary row (运行中, 策略总数, 今日成交, 今日已实现盈亏) as one ruled band, collapsible account and position panels side by side, then the strategy cards grid.
FORM: Sidebar plus ⌘K command palette; signal strip as the signature; ruled bands instead of nested cards. Position on ordering: 4 of 7 (roll-assigned).

SIGNATURE MOMENT: The signal strip, reused on the pair detail header, and the grid ladder where the mark price is drawn as a labelled front line between sell and buy levels.
