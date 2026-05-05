import { useEffect, useRef, useState } from 'react'
import type { IHeaderParams, SortDirection } from 'ag-grid-community'
import HoverTooltip from './HoverTooltip'

type SheetHeaderParams = IHeaderParams & {
  tooltipShowDelayMs?: number
}

type HeaderState = {
  filterActive: boolean
  sort: SortDirection | undefined
}

function readHeaderState(params: IHeaderParams): HeaderState {
  return {
    filterActive: params.column.isFilterActive(),
    sort: params.column.getSort(),
  }
}

function readFilterPopupOpenState(params: IHeaderParams) {
  return params.column.isMenuVisible()
}

function SortIcon({ sort }: { sort: SortDirection | undefined }) {
  if (sort === 'asc') {
    return (
      <svg viewBox="0 0 16 16" aria-hidden="true">
        <path d="M8 2.5 11.5 6H9.25v7.5h-2.5V6H4.5L8 2.5Z" fill="currentColor" />
      </svg>
    )
  }

  if (sort === 'desc') {
    return (
      <svg viewBox="0 0 16 16" aria-hidden="true">
        <path d="M8 13.5 4.5 10h2.25V2.5h2.5V10h2.25L8 13.5Z" fill="currentColor" />
      </svg>
    )
  }

  return (
    <svg viewBox="0 0 16 16" aria-hidden="true">
      <path d="M8 1.75 11 4.8H9.1v4.45H6.9V4.8H5L8 1.75Zm0 12.5L5 11.2h1.9V6.75h2.2v4.45H11L8 14.25Z" fill="currentColor" />
    </svg>
  )
}

function FilterIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true">
      <path
        d="M2.25 3.25A.75.75 0 0 1 3 2.5h10a.75.75 0 0 1 .56 1.25L9.5 8.28v3.18a.75.75 0 0 1-.37.65l-1.75 1a.75.75 0 0 1-1.13-.65V8.28L2.19 3.75a.75.75 0 0 1 .06-.5Z"
        fill="currentColor"
      />
    </svg>
  )
}

export default function SheetHeader(params: SheetHeaderParams) {
  const filterButtonRef = useRef<HTMLButtonElement | null>(null)
  const [state, setState] = useState<HeaderState>(() => readHeaderState(params))
  const [isFilterPopupOpen, setIsFilterPopupOpen] = useState(() => readFilterPopupOpenState(params))
  const wasOpenOnPressRef = useRef(false)
  const sortTooltip =
    state.sort === 'asc'
      ? `${params.displayName} sorted ascending`
      : state.sort === 'desc'
        ? `${params.displayName} sorted descending`
        : `Sort ${params.displayName}`
  const filterTooltip = state.filterActive ? `${params.displayName} filter active` : `Filter ${params.displayName}`

  useEffect(() => {
    const sync = () => setState(readHeaderState(params))
    const syncMenuVisible = () => setIsFilterPopupOpen(readFilterPopupOpenState(params))
    sync()
    syncMenuVisible()

    params.column.addEventListener('sortChanged', sync)
    params.column.addEventListener('filterActiveChanged', sync)
    params.column.addEventListener('colDefChanged', sync)
    params.column.addEventListener('menuVisibleChanged', syncMenuVisible)

    return () => {
      params.column.removeEventListener('sortChanged', sync)
      params.column.removeEventListener('filterActiveChanged', sync)
      params.column.removeEventListener('colDefChanged', sync)
      params.column.removeEventListener('menuVisibleChanged', syncMenuVisible)
    }
  }, [params])

  return (
    <div className="sheet-header">
      <HoverTooltip
        content={params.displayName}
        anchorClassName="sheet-header-title-tooltip"
        showDelayMs={params.tooltipShowDelayMs}
      >
        <span className="sheet-header-title">{params.displayName}</span>
      </HoverTooltip>

      <div className="sheet-header-actions">
        {params.enableSorting ? (
          <HoverTooltip content={sortTooltip} showDelayMs={params.tooltipShowDelayMs}>
            <button
              type="button"
              className={state.sort ? 'sheet-header-button active' : 'sheet-header-button'}
              aria-label={sortTooltip}
              onClick={(event) => {
                event.preventDefault()
                event.stopPropagation()
                params.progressSort(false)
              }}
            >
              <span className="sheet-header-button-icon">
                <SortIcon sort={state.sort} />
              </span>
            </button>
          </HoverTooltip>
        ) : null}

        {params.column.isFilterAllowed() ? (
          <HoverTooltip content={filterTooltip} showDelayMs={params.tooltipShowDelayMs}>
            <button
              ref={filterButtonRef}
              type="button"
              className={state.filterActive ? 'sheet-header-button active' : 'sheet-header-button'}
              aria-label={filterTooltip}
              aria-expanded={isFilterPopupOpen}
              onMouseDownCapture={() => {
                wasOpenOnPressRef.current = params.column.isMenuVisible()
              }}
              onTouchStartCapture={() => {
                wasOpenOnPressRef.current = params.column.isMenuVisible()
              }}
              onClick={(event) => {
                event.preventDefault()
                event.stopPropagation()
                const wasOpenOnPress = wasOpenOnPressRef.current
                wasOpenOnPressRef.current = false

                params.api.hidePopupMenu()

                if (wasOpenOnPress) {
                  return
                }

                if (filterButtonRef.current) {
                  params.showFilter(filterButtonRef.current)
                }
              }}
            >
              <span className="sheet-header-button-icon">
                <FilterIcon />
              </span>
            </button>
          </HoverTooltip>
        ) : null}
      </div>
    </div>
  )
}
