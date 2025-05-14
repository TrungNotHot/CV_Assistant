import { format, parseISO } from "date-fns";
import { z } from "zod";
import { formSearchSchema } from "./form-search-schema";

export const searchParamsSchema = formSearchSchema.extend({
    locations: formSearchSchema.shape.locations.transform((val) => val ?? null),
    levels: formSearchSchema.shape.levels.transform((val) => val ?? null),
    updatedAfter: z
        .string()
        .datetime()
        .nullish()
        .transform((val) => (val ? format(parseISO(val), "yyyy-MM-dd") : null)),
    lastFreq: z
        .string()
        .nullish()
        .transform((val) => (val ? parseFloat(val) : null))
        .default(null),

    lastName: z.string().nullish().default(null),
    limit: z
        .string()
        .optional()
        .transform((val) => (val ? parseInt(val) : 10))
        .refine((val) => val > 0 && val <= 100, {
            message: "Limit must be between 1 and 100",
        }),
});
