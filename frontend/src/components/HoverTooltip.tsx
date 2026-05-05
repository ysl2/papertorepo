import {
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import { createPortal } from 'react-dom'

type HoverTooltipProps = {
  content: ReactNode | null | undefined
  children: ReactNode
  anchorClassName?: string
  showDelayMs?: number
}

type TooltipPosition = {
  left: number
  top: number
}

const DEFAULT_TOOLTIP_SHOW_DELAY_MS = 200
const TOOLTIP_OFFSET_PX = 10
const TOOLTIP_VIEWPORT_GUTTER_PX = 10

function hasRenderableContent(content: ReactNode | null | undefined) {
  if (content == null || content === false) return false
  if (typeof content === 'string') return content.trim().length > 0
  return true
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max)
}

export default function HoverTooltip({
  content,
  children,
  anchorClassName,
  showDelayMs = DEFAULT_TOOLTIP_SHOW_DELAY_MS,
}: HoverTooltipProps) {
  const tooltipId = useId()
  const anchorRef = useRef<HTMLSpanElement | null>(null)
  const tooltipRef = useRef<HTMLDivElement | null>(null)
  const showTimerRef = useRef<number | null>(null)
  const [visible, setVisible] = useState(false)
  const [position, setPosition] = useState<TooltipPosition | null>(null)

  const enabled = hasRenderableContent(content)

  useEffect(() => {
    return () => {
      if (showTimerRef.current !== null) {
        window.clearTimeout(showTimerRef.current)
      }
    }
  }, [])

  useLayoutEffect(() => {
    if (!visible || !anchorRef.current || !tooltipRef.current || typeof window === 'undefined') return

    const updatePosition = () => {
      if (!anchorRef.current || !tooltipRef.current) return

      const anchorRect = anchorRef.current.getBoundingClientRect()
      const tooltipRect = tooltipRef.current.getBoundingClientRect()
      const minLeft = TOOLTIP_VIEWPORT_GUTTER_PX
      const maxLeft = window.innerWidth - tooltipRect.width - TOOLTIP_VIEWPORT_GUTTER_PX
      const centeredLeft = anchorRect.left + anchorRect.width / 2 - tooltipRect.width / 2
      const left = clamp(centeredLeft, minLeft, Math.max(minLeft, maxLeft))

      const preferredTop = anchorRect.top - tooltipRect.height - TOOLTIP_OFFSET_PX
      const fallbackTop = anchorRect.bottom + TOOLTIP_OFFSET_PX
      const top =
        preferredTop >= TOOLTIP_VIEWPORT_GUTTER_PX
          ? preferredTop
          : Math.min(
              fallbackTop,
              window.innerHeight - tooltipRect.height - TOOLTIP_VIEWPORT_GUTTER_PX,
            )

      setPosition({
        left: Math.round(left),
        top: Math.round(Math.max(TOOLTIP_VIEWPORT_GUTTER_PX, top)),
      })
    }

    updatePosition()
    window.addEventListener('resize', updatePosition)
    window.addEventListener('scroll', updatePosition, true)

    return () => {
      window.removeEventListener('resize', updatePosition)
      window.removeEventListener('scroll', updatePosition, true)
    }
  }, [content, visible])

  if (!enabled) {
    return <>{children}</>
  }

  function clearShowTimer() {
    if (showTimerRef.current !== null) {
      window.clearTimeout(showTimerRef.current)
      showTimerRef.current = null
    }
  }

  function scheduleShow() {
    clearShowTimer()
    showTimerRef.current = window.setTimeout(() => {
      showTimerRef.current = null
      setVisible(true)
    }, showDelayMs)
  }

  function hideTooltip() {
    clearShowTimer()
    setVisible(false)
  }

  return (
    <>
      <span
        ref={anchorRef}
        className={anchorClassName ? `hover-tooltip-anchor ${anchorClassName}` : 'hover-tooltip-anchor'}
        aria-describedby={visible ? tooltipId : undefined}
        onMouseEnter={scheduleShow}
        onMouseLeave={hideTooltip}
        onFocus={scheduleShow}
        onBlur={hideTooltip}
        onMouseDown={hideTooltip}
      >
        {children}
      </span>

      {visible && typeof document !== 'undefined'
        ? createPortal(
            <div
              ref={tooltipRef}
              id={tooltipId}
              role="tooltip"
              className="hover-tooltip-bubble"
              style={
                position
                  ? {
                      left: `${position.left}px`,
                      top: `${position.top}px`,
                    }
                  : {
                      left: '0px',
                      top: '0px',
                      visibility: 'hidden',
                    }
              }
            >
              <div className="hover-tooltip-surface">{content}</div>
            </div>,
            document.body,
          )
        : null}
    </>
  )
}
