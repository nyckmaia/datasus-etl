import i18n from "i18next";
import { initReactI18next } from "react-i18next";

import en from "./locales/en.json";
import pt from "./locales/pt.json";

export type Language = "pt" | "en";

const STORAGE_KEY = "datasus-language";

function readStoredLanguage(): Language {
  if (typeof window === "undefined") return "pt";
  const v = window.localStorage.getItem(STORAGE_KEY);
  return v === "en" || v === "pt" ? v : "pt";
}

void i18n.use(initReactI18next).init({
  resources: {
    pt: { translation: pt },
    en: { translation: en },
  },
  lng: readStoredLanguage(),
  fallbackLng: "pt",
  interpolation: { escapeValue: false },
  returnNull: false,
});

export function setLanguage(lang: Language): void {
  if (typeof window !== "undefined") {
    window.localStorage.setItem(STORAGE_KEY, lang);
    document.documentElement.lang = lang === "pt" ? "pt-BR" : "en";
  }
  void i18n.changeLanguage(lang);
}

if (typeof document !== "undefined") {
  const initial = readStoredLanguage();
  document.documentElement.lang = initial === "pt" ? "pt-BR" : "en";
}

export { STORAGE_KEY as LANGUAGE_STORAGE_KEY };
export default i18n;
