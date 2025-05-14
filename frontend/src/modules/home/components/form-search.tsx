"use client";

import { Button } from "@/components/ui/button";
import { Form, FormField, FormItem, FormMessage } from "@/components/ui/form";
import { useFilters } from "@/hooks/use-filters";
import { zodResolver } from "@hookform/resolvers/zod";
import { Loader2 } from "lucide-react";
import { useEffect } from "react";
import { useForm } from "react-hook-form";
import { getLevels } from "../apis/get-levels";
import { getLocations } from "../apis/get-locations";
import { useGetSkillsInfinite } from "../hooks/use-skill";
import { getMonthAgo } from "../lib/utils";
import {
    FormSearchData,
    formSearchSchema,
} from "../schemas/form-search-schema";
import { InputTitle } from "./input-title";
import { OptionsSelector } from "./options-selector";
import { SelectTime } from "./select-time";

export const FormSearch = () => {
    const form = useForm<FormSearchData>({
        resolver: zodResolver(formSearchSchema),
        defaultValues: {
            jobTitle: "",
            locations: [],
            levels: [],
        },
    });

    const [filters, setFilters] = useFilters();
    const { isLoading } = useGetSkillsInfinite();

    useEffect(() => {
        form.setValue("jobTitle", filters.jobTitle ?? "");
        form.setValue("locations", filters.locations ?? []);
        form.setValue("levels", filters.levels ?? []);
        form.setValue("updatedAfter", filters.updatedAfter ?? getMonthAgo(6));
    }, [filters, form]);

    const onSubmit = (data: FormSearchData) => {
        if (isLoading) {
            return;
        }
        setFilters(data);
    };

    return (
        <div className="mb-8 rounded-lg bg-white p-6 shadow-sm">
            <Form {...form}>
                <form
                    onSubmit={form.handleSubmit(onSubmit)}
                    className="space-y-6"
                >
                    <div className="flex gap-2">
                        <FormField
                            control={form.control}
                            name="jobTitle"
                            render={({ field }) => (
                                <FormItem className="flex-1">
                                    <InputTitle
                                        value={field.value}
                                        onChange={field.onChange}
                                    />
                                    <FormMessage />
                                </FormItem>
                            )}
                        />
                        <Button
                            type="submit"
                            disabled={isLoading}
                            className="min-w-[100px]"
                        >
                            {isLoading ? (
                                <span className="flex items-center gap-2">
                                    <Loader2 className="h-4 w-4 animate-spin" />
                                    <span>Searching</span>
                                </span>
                            ) : (
                                "Search"
                            )}
                        </Button>
                    </div>

                    <div className="grid grid-cols-1 gap-6 md:grid-cols-3">
                        <FormField
                            control={form.control}
                            name="levels"
                            render={({ field }) => (
                                <OptionsSelector
                                    label="Experience Level"
                                    values={field.value ?? []}
                                    onChange={field.onChange}
                                    queryKey={["levels"]}
                                    queryFn={getLevels}
                                />
                            )}
                        />
                        <FormField
                            control={form.control}
                            name="locations"
                            render={({ field }) => (
                                <OptionsSelector
                                    label="Location"
                                    values={field.value ?? []}
                                    onChange={field.onChange}
                                    queryKey={["locations"]}
                                    queryFn={getLocations}
                                />
                            )}
                        />
                        <FormField
                            control={form.control}
                            name="updatedAfter"
                            render={({ field }) => (
                                <SelectTime
                                    value={field.value}
                                    onChange={field.onChange}
                                />
                            )}
                        />
                    </div>
                </form>
            </Form>
        </div>
    );
};
