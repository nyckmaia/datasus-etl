import { useMutation } from "@tanstack/react-query";

import { api } from "@/lib/api";
import type { SqlRequest, SqlResult } from "@/lib/api";

export function useSqlQuery() {
  return useMutation<SqlResult, Error, SqlRequest>({
    mutationFn: (payload) => api.runSql(payload),
  });
}
