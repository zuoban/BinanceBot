import type { ReactNode } from 'react';
import { ConfigProvider, theme as antdTheme } from 'antd';
import type { Locale } from 'antd/es/locale';
import { componentColors, dimensions, fonts, palettes } from './designTokens';
import { ThemeStateProvider, useHasOuterThemeState, useThemeDark } from './themeState';
export { useTheme, type ThemePreference } from './themeState';

const typeTemplate = '${label}不是一个有效的${type}';
const zhCNBase: Locale = {
  locale: 'zh-cn',
  Pagination: {
    items_per_page: '条/页',
    jump_to: '跳至',
    jump_to_confirm: '确定',
    page: '页',
    prev_page: '上一页',
    next_page: '下一页',
    prev_5: '向前 5 页',
    next_5: '向后 5 页',
    prev_3: '向前 3 页',
    next_3: '向后 3 页',
    page_size: '页码',
  },
  global: {
    placeholder: '请选择',
    close: '关闭',
    sortable: '可排序',
    show: '显示',
    hide: '隐藏',
    clear: '清除',
  },
  Table: {
    filterTitle: '筛选',
    filterConfirm: '确定',
    filterReset: '重置',
    filterEmptyText: '无筛选项',
    filterCheckAll: '全选',
    filterSearchPlaceholder: '在筛选项中搜索',
    emptyText: '暂无数据',
    selectAll: '全选当页',
    selectInvert: '反选当页',
    selectNone: '清空所有',
    selectionAll: '全选所有',
    selectRow: '选择行',
    selectedRow: '行已选择',
    selectionMenu: '选择菜单',
    sortTitle: '排序',
    expand: '展开行',
    collapse: '关闭行',
    expandAll: '展开所有行',
    collapseAll: '收起所有行',
    triggerDesc: '点击降序',
    triggerAsc: '点击升序',
    cancelSort: '取消排序',
  },
  Modal: {
    okText: '确定',
    cancelText: '取消',
    justOkText: '知道了',
  },
  Popconfirm: {
    cancelText: '取消',
    okText: '确定',
  },
  Empty: {
    description: '暂无数据',
  },
  Text: {
    edit: '编辑',
    copy: '复制',
    copied: '复制成功',
    expand: '展开',
    collapse: '收起',
  },
  Form: {
    optional: '（可选）',
    defaultValidateMessages: {
      default: '字段验证错误${label}',
      required: '请输入${label}',
      enum: '${label}必须是其中一个[${enum}]',
      whitespace: '${label}不能为空字符',
      date: {
        format: '${label}日期格式无效',
        parse: '${label}不能转换为日期',
        invalid: '${label}是一个无效日期',
      },
      types: {
        string: typeTemplate,
        method: typeTemplate,
        array: typeTemplate,
        object: typeTemplate,
        number: typeTemplate,
        date: typeTemplate,
        boolean: typeTemplate,
        integer: typeTemplate,
        float: typeTemplate,
        regexp: typeTemplate,
        email: typeTemplate,
        url: typeTemplate,
        hex: typeTemplate,
      },
      string: {
        len: '${label}须为${len}个字符',
        min: '${label}最少${min}个字符',
        max: '${label}最多${max}个字符',
        range: '${label}须在${min}-${max}字符之间',
      },
      number: {
        len: '${label}必须等于${len}',
        min: '${label}最小值为${min}',
        max: '${label}最大值为${max}',
        range: '${label}须在${min}-${max}之间',
      },
      array: {
        len: '须为${len}个${label}',
        min: '最少${min}个${label}',
        max: '最多${max}个${label}',
        range: '${label}数量须在${min}-${max}之间',
      },
      pattern: {
        mismatch: '${label}与模式不匹配${pattern}',
      },
    },
  },
  Input: {
    otpCell: '第 ${index} 位，共 ${total} 位',
  },
};

function buildAntdTheme(dark: boolean) {
  const palette = palettes[dark ? 'dark' : 'light'];
  const colors = componentColors[dark ? 'dark' : 'light'];
  return {
    algorithm: dark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
    token: {
      colorPrimary: palette['--accent'],
      colorInfo: palette['--text-secondary'],
      colorInfoBg: palette['--bg-raised'],
      colorInfoBorder: colors.borderSecondary,
      colorSuccess: palette['--positive'],
      colorSuccessBg: palette['--sig-calm-bg'],
      colorSuccessBorder: palette['--sig-calm-line'],
      colorError: palette['--negative'],
      colorErrorBg: palette['--sig-red-bg'],
      colorErrorBorder: palette['--sig-red-line'],
      colorWarning: palette['--sig-yellow'],
      colorWarningBg: palette['--sig-yellow-bg'],
      colorWarningBorder: palette['--sig-yellow-line'],
      borderRadius: dimensions.radiusControl,
      borderRadiusSM: dimensions.radiusChip,
      borderRadiusLG: dimensions.radiusPanel,
      controlHeight: dimensions.controlHeight,
      controlHeightSM: dimensions.controlHeightSM,
      controlHeightLG: dimensions.controlTouch,
      fontSize: dimensions.textBase,
      fontSizeSM: dimensions.textXs,
      fontSizeLG: dimensions.textSection,
      fontFamily: fonts.sans,
      fontFamilyCode: fonts.mono,
      colorBgLayout: palette['--bg-page'],
      colorBgContainer: palette['--bg-card'],
      colorBgElevated: colors.elevated,
      colorBorder: colors.border,
      colorBorderSecondary: colors.borderSecondary,
      colorText: palette['--text-primary'],
      colorTextSecondary: palette['--text-secondary'],
      colorTextTertiary: palette['--text-muted'],
      colorTextPlaceholder: palette['--text-muted'],
      colorTextDisabled: colors.disabledText,
      colorBgContainerDisabled: colors.disabledBg,
      controlItemBgHover: colors.hover,
      controlItemBgActive: colors.active,
      controlItemBgActiveHover: colors.activeHover,
      controlOutline: palette['--accent-soft'],
      controlOutlineWidth: 2,
    },
    components: {
      Card: { borderRadiusLG: dimensions.radiusPanel, headerFontSize: dimensions.textSection, headerHeight: 52 },
      Table: {
        headerBg: colors.tableHeader,
        headerColor: palette['--text-secondary'],
        headerSplitColor: 'transparent',
        rowHoverBg: colors.tableHover,
        borderColor: palette['--border-hairline'],
      },
      Dropdown: { paddingBlock: dimensions.radiusChip, borderRadiusLG: dimensions.radiusPanel, borderRadiusSM: dimensions.radiusChip },
      Select: {
        optionPadding: `${dimensions.space2}px ${dimensions.space3}px`,
        optionSelectedFontWeight: 500,
        optionSelectedColor: palette['--text-primary'],
      },
      // High-contrast neutral primary actions keep the chrome calm while accent marks selection and moments.
      Button: {
        colorPrimary: colors.primaryBg,
        colorPrimaryHover: colors.primaryHover,
        colorPrimaryActive: colors.primaryActive,
        primaryColor: colors.buttonForeground,
        dangerColor: colors.dangerForeground,
      },
      Checkbox: { colorWhite: colors.buttonForeground },
      DatePicker: { colorTextLightSolid: colors.buttonForeground },
      Switch: { colorPrimary: '#9a6700', colorPrimaryHover: dark ? '#9f6b00' : '#8f5f00' },
      Tabs: { horizontalMargin: `0 0 ${dimensions.space5}px`, titleFontSize: dimensions.textBase },
      Segmented: { trackBg: colors.segmentedTrack },
    },
  };
}
const antdThemes = { light: buildAntdTheme(false), dark: buildAntdTheme(true) };

function AntdThemeBridge({ children }: { children: ReactNode }) {
  const dark = useThemeDark();
  return <ConfigProvider locale={zhCNBase} theme={antdThemes[dark ? 'dark' : 'light']}>{children}</ConfigProvider>;
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const hasOuterState = useHasOuterThemeState();
  const bridged = <AntdThemeBridge>{children}</AntdThemeBridge>;
  return hasOuterState ? bridged : <ThemeStateProvider>{bridged}</ThemeStateProvider>;
}
