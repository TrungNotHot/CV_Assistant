import { useQuery } from "@tanstack/react-query";
import { getLevels } from "../apis/get-levels";

export const useGetLevels = () => {
    return useQuery({
        queryKey: ["levels"],
        queryFn: getLevels,
    });
};
