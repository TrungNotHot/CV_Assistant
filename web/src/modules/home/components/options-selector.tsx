import { Button } from "@/components/ui/button";
import {
    Command,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
} from "@/components/ui/command";
import { FormItem } from "@/components/ui/form";
import { Label } from "@/components/ui/label";
import {
    Popover,
    PopoverContent,
    PopoverTrigger,
} from "@/components/ui/popover";
import { useQuery } from "@tanstack/react-query";
import { Check, ChevronsUpDown, Loader2, TriangleAlert, X } from "lucide-react";
import { useState } from "react";
import { isMatch } from "../lib/utils";

type Props = {
    label: string;
    values: string[];
    onChange: (newValues: string[]) => void;
    queryKey: string[];
    queryFn: () => Promise<string[]>;
};

export const OptionsSelector = ({
    values,
    label,
    onChange,
    queryKey,
    queryFn,
}: Props) => {
    const [open, setOpen] = useState(false);

    const { data, isLoading, isError, error } = useQuery({
        queryKey,
        queryFn,
    });

    const toggleOption = (option: string) => {
        if (values.includes(option)) {
            onChange(values.filter((l) => l !== option));
        } else {
            onChange([...values, option]);
        }
    };

    if (isLoading) {
        return <OptionsSelectorSkeleton label={label} />;
    }

    if (!data || isError) {
        return <OptionsSelectorError label={label} error={error} />;
    }

    const onClearOption = () => {
        onChange([]);
        setOpen(false);
    };

    return (
        <FormItem>
            <Label>{label}</Label>
            <Popover open={open} onOpenChange={setOpen}>
                <PopoverTrigger asChild>
                    <Button
                        variant="outline"
                        role="combobox"
                        aria-expanded={open}
                        className="relative h-10 w-full justify-between px-4"
                    >
                        {values.length > 0
                            ? `${values.length} selected`
                            : `Select or search...`}
                        <div className="flex items-center">
                            {values.length > 0 ? (
                                <div
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        onClearOption();
                                    }}
                                >
                                    <X className="text-muted-foreground mr-1 h-4 w-4" />
                                </div>
                            ) : (
                                <ChevronsUpDown className="h-4 w-4 shrink-0 opacity-50" />
                            )}
                        </div>
                    </Button>
                </PopoverTrigger>
                <PopoverContent className="w-[250px] p-0">
                    <Command
                        filter={(value, search) => {
                            const match = isMatch(value, search);

                            return match ? 1 : 0;
                        }}
                    >
                        <CommandInput
                            placeholder={`Search ${label.toLowerCase()}...`}
                            className="py-3"
                        />
                        <CommandList>
                            <CommandEmpty>
                                No {label.toLowerCase()} found.
                            </CommandEmpty>
                            <CommandGroup className="p-2">
                                {data.map((option) => (
                                    <CommandItem
                                        key={option}
                                        value={option}
                                        onSelect={() => toggleOption(option)}
                                    >
                                        <div className="flex w-full items-center">
                                            <span className="flex-1">
                                                {option}
                                            </span>
                                            {values.includes(option) && (
                                                <Check className="ml-auto h-4 w-4" />
                                            )}
                                        </div>
                                    </CommandItem>
                                ))}
                            </CommandGroup>
                        </CommandList>
                    </Command>
                </PopoverContent>
            </Popover>
        </FormItem>
    );
};

export const OptionsSelectorSkeleton = ({ label }: { label: string }) => {
    return (
        <FormItem>
            <Label>{label}</Label>
            <Button
                disabled
                variant="outline"
                className="relative h-10 w-full justify-between px-4"
            >
                Loading {label.toLowerCase()}...
                <Loader2 className="text-muted-foreground mr-1 h-4 w-4 animate-spin" />
            </Button>
        </FormItem>
    );
};

export const OptionsSelectorError = ({
    label,
    error,
}: {
    label: string;
    error: Error | null;
}) => {
    return (
        <FormItem>
            <Label>{label}</Label>
            <Button
                disabled
                variant="outline"
                className="justify-between border-red-500 px-4 text-red-500 !opacity-100"
            >
                {error?.message || "Error loading options"}
                <TriangleAlert className="mr-1 h-4 w-4 text-red-500" />
            </Button>
        </FormItem>
    );
};
