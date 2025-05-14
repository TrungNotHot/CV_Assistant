"use client";

import { useFilters } from "@/hooks/use-filters";
import { InitialState } from "./initial-state";
import { SkillResults } from "./skill-results";

export const SkillResultsWrapper = () => {
    const [filters] = useFilters();

    const hasSearched = !!filters.jobTitle;

    if (!hasSearched) {
        return <InitialState />;
    }

    return <SkillResults />;
};
