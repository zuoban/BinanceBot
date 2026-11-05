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

THESIS: The console reads like a weather bulletin for a market: calm default, and four graded signal tiers (blue / yellow / orange / red) carry every abnormal condition with the same icon-plus-word grammar. Refuses the category default of a grey dashboard with one accent and scattered red/green badges.

OWN-WORLD: Deep marine-ink rail (always dark) beside a cool pale-sky working ground; signal blue as the single action colour; tiered signal colours reserved for state; hairline 1px rules, 8px controls, 12px panels, no shadows except overlays. System CJK sans for text, tabular monospace for every figure. Dense bulletin rhythm: small caps-free labels, figure columns right-aligned, rules between rows instead of boxes inside boxes.

STORY: The visitor sees in one glance whether anything needs attention, then drills to a pair in one click from the rail or ⌘K, and acts with confirmation where money is at risk.

FIRST VIEWPORT: Left rail 232px (brand, 总览, 设置, live pair list with status dots and direction arrows, mode chip, theme and logout at the foot). Top of content: page title with the signal strip directly under it (the signature move: tiered chips for 数据过期, 实盘, 已暂停 n, 异常 n, each tier-coloured with icon and count; calm state shows a single quiet 一切正常 chip). Then a four-figure summary row (运行中, 今日成交, 今日盈亏, 持仓金额) as one ruled band, account and position panels side by side, then the strategy table-cards grid.

FORM: Sidebar plus ⌘K command palette; signal strip as the signature; ruled bands instead of nested cards. Position on ordering: 4 of 7 (roll-assigned).

SIGNATURE MOMENT: The signal strip, reused on the pair detail header, and the grid ladder where the mark price is drawn as a labelled front line between sell and buy levels.
