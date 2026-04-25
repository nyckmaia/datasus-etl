import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { Toaster } from "sonner";

import "./globals.css";
import "@/i18n";

import { queryClient } from "@/lib/query";
import { router } from "@/router";
import { ThemeProvider } from "@/components/ThemeProvider";

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

const rootEl = document.getElementById("root");
if (!rootEl) {
  throw new Error("#root element not found");
}

ReactDOM.createRoot(rootEl).render(
  <React.StrictMode>
    <ThemeProvider defaultTheme="dark" storageKey="datasus-theme">
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
        <Toaster richColors position="top-right" theme="system" />
      </QueryClientProvider>
    </ThemeProvider>
  </React.StrictMode>,
);
