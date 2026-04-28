import * as React from "react";
import { Plus, X } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  DndContext,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  SortableContext,
  horizontalListSortingStrategy,
  useSortable,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";

import { cn } from "@/lib/utils";

export interface QueryTab {
  id: string;
  name: string;
}

interface QueryTabsBarProps {
  tabs: QueryTab[];
  activeId: string;
  onActivate: (id: string) => void;
  onClose: (id: string) => void;
  onAdd: () => void;
  onReorder: (orderedIds: string[]) => void;
  onRename: (id: string, name: string) => void;
}

export function QueryTabsBar({
  tabs,
  activeId,
  onActivate,
  onClose,
  onAdd,
  onReorder,
  onRename,
}: QueryTabsBarProps) {
  const { t } = useTranslation();

  const sensors = useSensors(
    // 5px activation distance prevents an accidental drag from swallowing
    // a click — click-to-activate must keep working with no flicker.
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
  );

  const onDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) return;
    const oldIndex = tabs.findIndex((tab) => tab.id === active.id);
    const newIndex = tabs.findIndex((tab) => tab.id === over.id);
    if (oldIndex < 0 || newIndex < 0) return;
    const next = [...tabs];
    const [moved] = next.splice(oldIndex, 1);
    next.splice(newIndex, 0, moved);
    onReorder(next.map((tab) => tab.id));
  };

  return (
    <div className="flex items-center gap-0.5 border-b bg-muted/30 px-1.5 pt-1">
      <DndContext
        sensors={sensors}
        collisionDetection={closestCenter}
        onDragEnd={onDragEnd}
      >
        <SortableContext
          items={tabs.map((tab) => tab.id)}
          strategy={horizontalListSortingStrategy}
        >
          <div className="flex min-w-0 flex-1 items-end gap-0.5 overflow-x-auto">
            {tabs.map((tab) => (
              <SortableTab
                key={tab.id}
                tab={tab}
                active={tab.id === activeId}
                canClose={tabs.length > 1}
                onActivate={() => onActivate(tab.id)}
                onClose={() => onClose(tab.id)}
                onRename={(name) => onRename(tab.id, name)}
              />
            ))}
          </div>
        </SortableContext>
      </DndContext>
      <button
        type="button"
        onClick={onAdd}
        title={t("query.tabs.newTab")}
        aria-label={t("query.tabs.newTab")}
        className="ml-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-secondary hover:text-foreground"
      >
        <Plus className="h-4 w-4" />
      </button>
    </div>
  );
}

interface SortableTabProps {
  tab: QueryTab;
  active: boolean;
  canClose: boolean;
  onActivate: () => void;
  onClose: () => void;
  onRename: (name: string) => void;
}

function SortableTab({
  tab,
  active,
  canClose,
  onActivate,
  onClose,
  onRename,
}: SortableTabProps) {
  const { t } = useTranslation();
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: tab.id });

  const [editing, setEditing] = React.useState(false);
  const [draft, setDraft] = React.useState(tab.name);
  const inputRef = React.useRef<HTMLInputElement>(null);

  // When the tab name changes externally (e.g. another rename or persisted
  // value rehydrated), keep the local draft in sync — but only when not
  // actively editing, so the user's keystrokes are never overwritten.
  React.useEffect(() => {
    if (!editing) setDraft(tab.name);
  }, [tab.name, editing]);

  React.useEffect(() => {
    if (editing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [editing]);

  const beginRename = () => {
    setDraft(tab.name);
    setEditing(true);
  };

  const commitRename = () => {
    const next = draft.trim();
    // Empty / whitespace-only → keep the previous name. The user explicitly
    // asked for this fallback so a tab is never displayed without a name.
    if (next && next !== tab.name) {
      onRename(next);
    }
    setEditing(false);
    setDraft(tab.name);
  };

  const cancelRename = () => {
    setEditing(false);
    setDraft(tab.name);
  };

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    zIndex: isDragging ? 10 : undefined,
  };

  // The input grows with its content. `ch` ≈ width of "0", which is close
  // enough for proportional fonts at this scale; +1 leaves room for the
  // text caret. Floor/ceiling guard against pathological inputs.
  const inputWidthCh = Math.max(4, Math.min(48, draft.length + 1));

  // While editing, suppress the dnd listeners so typing/click inside the
  // input never starts a drag — but keep `setNodeRef` and `attributes` so
  // sortable still tracks the element's position in the list.
  const dragListeners = editing ? undefined : listeners;

  return (
    <div
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...dragListeners}
      onClick={editing ? undefined : onActivate}
      onDoubleClick={editing ? undefined : beginRename}
      onKeyDown={(event) => {
        if (editing) return;
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onActivate();
        } else if (event.key === "F2") {
          event.preventDefault();
          beginRename();
        }
      }}
      role="tab"
      aria-selected={active}
      tabIndex={0}
      className={cn(
        "group flex h-8 shrink-0 select-none items-center gap-1.5 rounded-t-md border border-b-0 px-2.5 text-xs outline-none transition-colors",
        editing ? "cursor-text" : "cursor-pointer",
        active
          ? "border-border bg-card font-medium text-foreground shadow-[0_1px_0_0_hsl(var(--card))]"
          : "border-transparent text-muted-foreground hover:bg-secondary/60 hover:text-foreground",
        isDragging && "opacity-70 shadow-md",
      )}
    >
      {editing ? (
        <input
          ref={inputRef}
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          onBlur={commitRename}
          onKeyDown={(event) => {
            // Keep keys local to the input — Enter/Escape control the rename
            // lifecycle, every other key just edits the value.
            event.stopPropagation();
            if (event.key === "Enter") {
              event.preventDefault();
              commitRename();
            } else if (event.key === "Escape") {
              event.preventDefault();
              cancelRename();
            }
          }}
          // Stop pointer events so the dnd PointerSensor on the parent
          // never sees the input's drag-y mouse motion (selecting text
          // would otherwise look like a drag intent).
          onPointerDown={(event) => event.stopPropagation()}
          onClick={(event) => event.stopPropagation()}
          onDoubleClick={(event) => event.stopPropagation()}
          aria-label={t("query.tabs.rename")}
          spellCheck={false}
          autoComplete="off"
          style={{ width: `${inputWidthCh}ch` }}
          className="min-w-[4ch] bg-transparent text-xs font-medium text-foreground outline-none"
        />
      ) : (
        <span className="whitespace-nowrap">{tab.name}</span>
      )}
      {canClose ? (
        <button
          type="button"
          onClick={(event) => {
            // Stop the click from bubbling up to the tab activator and from
            // initiating a drag on the parent.
            event.stopPropagation();
            onClose();
          }}
          onPointerDown={(event) => event.stopPropagation()}
          aria-label={t("query.tabs.close")}
          title={t("query.tabs.close")}
          className={cn(
            "flex h-4 w-4 shrink-0 items-center justify-center rounded transition-colors",
            "hover:bg-destructive/15 hover:text-destructive",
            active ? "opacity-80" : "opacity-0 group-hover:opacity-80",
          )}
        >
          <X className="h-3 w-3" />
        </button>
      ) : (
        // Reserve space so single-tab width matches multi-tab width — keeps
        // the strip stable as the user opens/closes tabs.
        <span className="h-4 w-4 shrink-0" />
      )}
    </div>
  );
}
