import { useTranslation } from "react-i18next";

import { cn } from "@/lib/utils";
import { setLanguage, type Language } from "@/i18n";

const FlagBR = ({ className }: { className?: string }) => (
  <svg
    viewBox="0 0 60 42"
    xmlns="http://www.w3.org/2000/svg"
    className={className}
    aria-hidden="true"
  >
    <rect width="60" height="42" fill="#009c3b" />
    <polygon points="30,4 56,21 30,38 4,21" fill="#ffdf00" />
    <circle cx="30" cy="21" r="8" fill="#002776" />
    <path
      d="M22.5 20.5 Q30 16 37.5 20.5"
      stroke="#fff"
      strokeWidth="1.6"
      fill="none"
    />
  </svg>
);

const FlagUS = ({ className }: { className?: string }) => (
  <svg
    viewBox="0 0 60 42"
    xmlns="http://www.w3.org/2000/svg"
    className={className}
    aria-hidden="true"
  >
    <rect width="60" height="42" fill="#fff" />
    {Array.from({ length: 7 }).map((_, i) => (
      <rect key={i} y={i * 6} width="60" height="3" fill="#b22234" />
    ))}
    <rect width="26" height="22" fill="#3c3b6e" />
    {Array.from({ length: 5 }).map((_, row) =>
      Array.from({ length: row % 2 === 0 ? 6 : 5 }).map((_, col) => (
        <circle
          key={`${row}-${col}`}
          cx={2 + col * 4 + (row % 2) * 2}
          cy={3 + row * 4}
          r="1"
          fill="#fff"
        />
      )),
    )}
  </svg>
);

export function LanguageToggle() {
  const { i18n, t } = useTranslation();
  const current = (i18n.resolvedLanguage ?? i18n.language ?? "pt") as Language;

  const buttonClass = (lang: Language) =>
    cn(
      "flex items-center gap-1.5 rounded-md px-2 py-1 text-xs font-medium transition-colors",
      current === lang
        ? "bg-secondary text-foreground"
        : "text-muted-foreground hover:bg-secondary/60 hover:text-foreground",
    );

  return (
    <div
      className="flex items-center gap-1 rounded-md border bg-background p-0.5"
      role="group"
      aria-label={t("language.label")}
    >
      <button
        type="button"
        onClick={() => setLanguage("en")}
        className={buttonClass("en")}
        aria-label={t("language.english")}
        aria-pressed={current === "en"}
        title={t("language.english")}
      >
        <FlagUS className="h-3.5 w-5 rounded-sm" />
        <span>EN</span>
      </button>
      <button
        type="button"
        onClick={() => setLanguage("pt")}
        className={buttonClass("pt")}
        aria-label={t("language.portuguese")}
        aria-pressed={current === "pt"}
        title={t("language.portuguese")}
      >
        <FlagBR className="h-3.5 w-5 rounded-sm" />
        <span>PT</span>
      </button>
    </div>
  );
}
