import { useFilters } from "@/hooks/use-filters";
import { useInfiniteQuery } from "@tanstack/react-query";
import queryString from "query-string";
import { Cursor, SkillApiResponse } from "../types";

export const useGetSkillsInfinite = () => {
    const [filters] = useFilters();

    return useInfiniteQuery({
        queryKey: ["skills", filters],
        enabled: !!filters.jobTitle,
        initialPageParam: null,
        queryFn: async ({ pageParam }: { pageParam: Cursor }) => {
            const searchParams = queryString.stringify(
                {
                    ...filters,
                    updatedAfter: filters.updatedAfter?.toISOString(),
                    lastFreq: pageParam?.lastFreq,
                    lastName: pageParam?.lastName,
                },
                {
                    arrayFormat: "bracket",
                    skipNull: true,
                },
            );

            const res = await fetch(`api/skills?${searchParams}`);

            if (!res.ok) {
                throw new Error("Failed to fetch skills");
            }

            return res.json() as Promise<SkillApiResponse>;
        },
        getNextPageParam: (lastPage) => lastPage.nextCursor,
    });
};
