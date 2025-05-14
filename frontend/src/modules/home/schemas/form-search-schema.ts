import { z } from "zod";

export const formSearchSchema = z.object({
    jobTitle: z.string().min(1, { message: "Job title is required" }),
    locations: z.array(z.string()).optional(),
    levels: z.array(z.string()).optional(),
    updatedAfter: z.date().nullish(),
});

export type FormSearchData = z.infer<typeof formSearchSchema>;
