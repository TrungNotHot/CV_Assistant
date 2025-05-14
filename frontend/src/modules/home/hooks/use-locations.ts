import { useQuery } from "@tanstack/react-query";
import { getLocations } from "../apis/get-locations";

export const useGetLocations = () => {
    return useQuery({
        queryKey: ["locations"],
        queryFn: getLocations,
    });
};
