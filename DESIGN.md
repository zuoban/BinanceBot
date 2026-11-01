---
name: Grid Console
description: 币安合约网格策略的交易工作台
colors:
  pine: "#176e59"
  pine-dark: "#174d40"
  mist: "#f7f7f6"
  surface: "#ffffff"
  text: "#242725"
  muted: "#626a65"
  border: "#e3e6e3"
  long: "#238367"
  short: "#c26b55"
  dark-background: "#141415"
  dark-surface: "#1c1d1d"
  dark-text: "#ededed"
  dark-primary: "#8cc9ae"
typography:
  body:
    fontFamily: "DM Sans, PingFang SC, sans-serif"
  headline:
    fontFamily: "DM Sans, PingFang SC, sans-serif"
    fontSize: "30px"
    fontWeight: 500
    letterSpacing: "0"
  numeric:
    fontFamily: "DM Mono, monospace"
    fontWeight: 500
  label:
    fontFamily: "DM Mono, monospace"
    fontSize: "10px"
    fontWeight: 500
    letterSpacing: "0.11em"
rounded:
  control: "8px"
  card: "12px"
  banner: "14px"
spacing:
  compact: "8px"
  standard: "16px"
  section: "20px"
  content: "44px"
---

# Design System: Grid Console

## Overview

**Creative North Star: “沉稳的交易工作台”**

界面沉稳、清晰，以任务和数据可读性为先。信息密度适中，使用克制的色彩、边框和卡片组织账户与策略信息；按钮、卡片和输入框保持清晰、便于操作。

**Key Characteristics:**
- 中性石墨色与浅灰色构成工作台基础，绿色和暖陶红用于操作、状态及交易方向。
- 数字使用等宽字体和表格数字，便于扫描比较。
- 浅色、深色和跟随系统三种主题共存。

## Colors

中性表面与细边框组织工作台，绿色用于主要操作，暖陶红表达做空及负值。深色主题以石墨色背景、面板和浮层区分层级。状态同时配合文字、图标或标签表达。

## Typography

页面与 Ant Design 控件使用 DM Sans，并以苹方和微软雅黑等字体回退。数值和英文辅助标签使用 DM Mono。页面标题为 30px、500 字重；区块标题为 19px；常见辅助文字为 12px。金额和统计数字采用等宽或 tabular-nums，避免位数变化造成跳动。

## Layout

顶部栏通过菜单提供主导航，内容最大宽度 1600px；窄屏内容水平内边距缩至 16px。表单和设置区域最大宽度为 1050px。窄屏表格允许横向滚动并提示。

## Elevation & Depth

平面为主，以中性色面和细边框分层；浮层按需使用轻微中性阴影。深色主题优先使用表面色阶区分层级。

## Shapes

控件使用 8px 圆角，卡片使用 12px，概览横幅使用 14px。交易方向通过文字与语义色共同表达，分隔线用于组织指标与操作。

## Components

### 导航

顶部栏菜单提供总览、设置、主题切换和退出；策略详情页还提供交易对切换。

### 卡片与数据

Ant Design 卡片承载账户、持仓、策略与图表。策略卡用绿色或暖陶红区分方向，内部按身份、价格、指标和操作分组。数字使用 DM Mono；重要状态还需可读文字。

策略运行概览按标题、核心指标、成交与状态明细排列；桌面指标使用四列，平板和手机使用两列，长数值允许换行。

页面加载占位沿用卡片表面与边框，以标题、指标和内容分组提示最终结构；总览、策略详情和通用页面按对应断点重排。浅色与深色主题使用静态中性色占位，避免动态效果干扰数据扫描。

成交记录未核实时，盈亏、手续费和成交方式显示“待同步”，避免将临时值当作实际成交数据。

总览成交汇总按笔数和分组盈亏分行排列。移动端成交卡片显示相对时间、成交价格、金额和盈亏，隐藏模式、数量、手续费、成交方式和备注；桌面表格保留完整字段，较窄屏幕提示横向滚动。

### 控件与反馈

按钮、输入框、选择器、标签和确认对话框基于 Ant Design，沿用主题色和圆角。筛选选项支持展开；有未保存修改的表单提供固定保存栏。焦点样式保持可见，减少动态效果设置会关闭骨架动画并缩短过渡。

## Do's and Don'ts

### Do:
- **Do** 保持浅色和深色主题中的状态及数据可读性。
- **Do** 用文字或图标补充交易方向和状态色。
- **Do** 保持数字对齐、支持窄屏及长数值换行。

### Don't:
- **Don't** 仅凭颜色传达方向、风险或运行状态。
- **Don't** 让装饰或动画妨碍数据扫描及操作。
