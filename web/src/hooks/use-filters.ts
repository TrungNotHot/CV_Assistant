import {
    parseAsArrayOf,
    parseAsIsoDate,
    parseAsString,
    useQueryStates,
} from "nuqs";

export const useFilters = () => {
    return useQueryStates({
        jobTitle: parseAsString,
        locations: parseAsArrayOf(parseAsString).withDefault([]),
        levels: parseAsArrayOf(parseAsString).withDefault([]),
        updatedAfter: parseAsIsoDate,
    });
};
