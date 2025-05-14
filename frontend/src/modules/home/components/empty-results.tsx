"use client";

import { useFilters } from "@/hooks/use-filters";
import { SearchX } from "lucide-react";

export const EmptyResults = () => {
    const [filters] = useFilters();

    return (
        <div className="rounded-lg bg-white p-8 text-center shadow-sm">
            <div className="mx-auto max-w-lg">
                <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-gray-100">
                    <SearchX className="h-8 w-8 text-gray-500" />
                </div>

                <h2 className="mb-2 text-2xl font-bold">No results found</h2>
                <p className="mb-6 text-sm text-gray-500">
                    We couldn&apos;t find any skills data for
                    <span className="font-semibold">
                        {filters?.jobTitle && ` "${filters.jobTitle}"`}
                    </span>
                    with your current filters.
                </p>
            </div>
        </div>
    );
};
