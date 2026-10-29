---
name: Grid Console
description: 币安合约网格策略的交易工作台
colors:
  pine: "#176e59"
  pine-dark: "#174d40"
  mist: "#f4f6f3"
  surface: "#ffffff"
  text: "#23352f"
  muted: "#66796d"
  border: "#e8ede9"
  long: "#238367"
  short: "#c26b55"
  dark-background: "#101a17"
  dark-surface: "#192723"
  dark-text: "#dce9e1"
  dark-primary: "#69bb98"
typography:
  body:
    fontFamily: "DM Sans, PingFang SC, sans-serif"
  headline:
    fontFamily: "DM Sans, PingFang SC, sans-serif"
    fontSize: "30px"
    fontWeight: 700
    letterSpacing: "-0.035em"
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
- 深松绿与雾灰绿构成工作台的基础，暖陶红区分做空及负值。
- 数字使用等宽字体和表格数字，便于扫描比较。
- 浅色、深色和跟随系统三种主题共存。

## Colors

深松绿用于主要操作和品牌，雾灰绿与白色构成浅色界面，暖陶红表达做空及负值。深色主题使用深绿黑背景与较亮的绿色、珊瑚色，保持语义区分。状态同时配合文字、图标或标签表达。

## Typography

页面与 Ant Design 控件使用 DM Sans，并以苹方和微软雅黑等字体回退。数值和英文辅助标签使用 DM Mono。页面标题为 30px、700 字重；区块标题为 19px；常见辅助文字为 12px。金额和统计数字采用等宽或 tabular-nums，避免位数变化造成跳动。

## Layout

桌面侧栏宽 232px，顶部栏高 68px；内容最大宽度 1600px，桌面内边距为 40px 44px 60px。991px 以下隐藏侧栏，改用抽屉导航，内容水平内边距缩至 20px；600px 以下进一步压缩卡片、图表和标题间距。表单和设置区域最大宽度为 1050px。页面允许窄屏表格横向滚动并提示。

## Elevation & Depth

平面为主，以边框、色面和轻微渐变分层；阴影用于交互和浮层。策略卡悬停使用 `0 12px 30px #213e2f0d` 并上移 2px；浮动保存栏使用 `0 8px 28px #2a40321a`，深色主题使用 `0 8px 28px #0005`。

## Shapes

控件使用 8px 圆角，卡片使用 12px，概览横幅使用 14px。策略卡以 3px 左边线和淡色背景提示交易方向；分隔线用于组织指标与操作。

## Components

### 导航

桌面使用固定侧栏和顶部栏，当前菜单以浅绿底色标记；窄屏使用左侧抽屉。顶部栏提供交易对切换、主题切换和退出。

### 卡片与数据

Ant Design 卡片承载账户、持仓、策略与图表。策略卡用绿色或暖陶红区分方向，内部按身份、价格、指标和操作分组。数字使用 DM Mono；重要状态还需可读文字。

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
