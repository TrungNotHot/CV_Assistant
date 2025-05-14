"use client";

import { Button } from "@/components/ui/button";
import { useFilters } from "@/hooks/use-filters";
import { Search, TrendingUp } from "lucide-react";
import { useRouter } from "next/navigation";
import { POPULAR_SEARCHES } from "../constants";
import { useGetLevels } from "../hooks/use-levels";
import { useGetLocations } from "../hooks/use-locations";
import { getMonthAgo } from "../lib/utils";

export const InitialState = () => {
    const locations = useGetLocations();
    const levels = useGetLevels();
    const router = useRouter();

    const isLoading = locations.isLoading || levels.isLoading;

    const [filters, setFilters] = useFilters();

    const onQuickSearch = (search: string) => {
        if (isLoading) return;

        const fallbackLocation = locations.data[0];
        const fallbackLevel = levels.data[0];

        if (!fallbackLevel || !fallbackLocation) return;
        setFilters({
            jobTitle: search,
            locations: [fallbackLocation],
            levels: [fallbackLevel],
            updatedAfter: getMonthAgo(6),
        });
    };

    return (
        <div className="rounded-lg bg-white p-8 text-center shadow-sm">
            <div className="mx-auto max-w-md">
                <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-gray-100">
                    <Search className="h-8 w-8 text-gray-500" />
                </div>

                <h2 className="mb-2 text-2xl font-bold">
                    Start exploring job skills
                </h2>
                <p className="mb-6 text-gray-500">
                    Enter a job title in the search box above and discover the
                    most in-demand skills for that position.
                </p>

                <div className="mb-8">
                    <div className="mb-3 flex items-center justify-center gap-2">
                        <TrendingUp className="h-5 w-5 text-gray-500" />
                        <h3 className="font-medium">Popular searches</h3>
                    </div>
                    <div className="flex flex-wrap justify-center gap-2">
                        {POPULAR_SEARCHES.map((search) => (
                            <Button
                                key={search}
                                variant="outline"
                                size="sm"
                                onClick={() => onQuickSearch(search)}
                                disabled={isLoading}
                            >
                                {search}
                            </Button>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    );
};
