import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import { FormItem } from "@/components/ui/form";
import { Label } from "@/components/ui/label";
import {
    Popover,
    PopoverContent,
    PopoverTrigger,
} from "@/components/ui/popover";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";
import { format } from "date-fns";
import { Calendar as CalendarIcon, X } from "lucide-react";
import { useState } from "react";
import { TIME_PRESETS } from "../constants";
import { getMonthAgo } from "../lib/utils";

type Props = {
    value?: Date | null;
    onChange: (newValue?: Date | null) => void;
};

export const SelectTime = ({ value, onChange }: Props) => {
    const [open, setOpen] = useState(false);

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const isDateInFuture = (value: Date) => {
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        return value > today;
    };

    const isToday = (someDate?: Date): boolean => {
        if (!someDate) return false;
        const today = new Date();
        return (
            someDate.getDate() === today.getDate() &&
            someDate.getMonth() === today.getMonth() &&
            someDate.getFullYear() === today.getFullYear()
        );
    };

    const getCurrentPresetValue = (): string => {
        if (!value) return "0";
        if (isToday(value)) return "0";

        const now = new Date();
        const diffMonths =
            (now.getFullYear() - value.getFullYear()) * 12 +
            now.getMonth() -
            value.getMonth();

        if (diffMonths >= 23 && diffMonths <= 25) return "24";
        if (diffMonths >= 11 && diffMonths <= 13) return "12";
        if (diffMonths >= 5 && diffMonths <= 7) return "6";
        if (diffMonths >= 2 && diffMonths <= 4) return "3";

        return "";
    };

    const onTimePresetChange = (monthsAgo: string) => {
        const newDate = getMonthAgo(Number.parseInt(monthsAgo));

        if (value) {
            newDate.setDate(value.getDate());
        }

        onChange(newDate);
    };

    const onMonthChange = (newMonth: Date) => {
        const newDate = new Date(value || new Date());
        newDate.setMonth(newMonth.getMonth());
        newDate.setFullYear(newMonth.getFullYear());

        if (isDateInFuture(newDate)) {
            const lastValidDay = new Date(newDate);
            while (isDateInFuture(lastValidDay) && lastValidDay.getDate() > 1) {
                lastValidDay.setDate(lastValidDay.getDate() - 1);
            }

            if (!isDateInFuture(lastValidDay)) {
                newDate.setDate(lastValidDay.getDate());
            } else {
                return;
            }
        }

        onChange(newDate);
    };

    const onDateSelect = (newDate?: Date) => {
        if (newDate && !isDateInFuture(newDate)) {
            onChange(newDate);
        }
    };

    const onClearDate = () => {
        onChange(null);
        setOpen(false);
    };

    return (
        <FormItem>
            <Label>Updated After</Label>
            <Popover open={open} onOpenChange={setOpen}>
                <PopoverTrigger asChild>
                    <Button
                        variant={"outline"}
                        className={cn(
                            "w-full justify-start text-left font-normal",
                            !value && "text-muted-foreground",
                        )}
                    >
                        <CalendarIcon />
                        {value ? (
                            <div className="flex w-full items-center justify-between">
                                <span>{format(value, "PPP")}</span>
                                <div
                                    className="flex items-center"
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        onClearDate();
                                    }}
                                >
                                    <X className="text-muted-foreground ml-auto h-4 w-4" />
                                </div>
                            </div>
                        ) : (
                            <span>Pick a value</span>
                        )}
                    </Button>
                </PopoverTrigger>
                <PopoverContent className="flex w-auto flex-col space-y-2 p-2">
                    <Select
                        value={getCurrentPresetValue()}
                        onValueChange={onTimePresetChange}
                    >
                        <SelectTrigger className="w-full">
                            <SelectValue placeholder="Time period" />
                        </SelectTrigger>
                        <SelectContent position="popper">
                            {TIME_PRESETS.map((preset) => (
                                <SelectItem
                                    key={preset.value}
                                    value={preset.value}
                                >
                                    {preset.label}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                    <div className="rounded-md border">
                        <Calendar
                            mode="single"
                            selected={value ?? undefined}
                            onSelect={onDateSelect}
                            initialFocus
                            month={value || new Date()}
                            onMonthChange={onMonthChange}
                            disabled={(value) => isDateInFuture(value)}
                            fromMonth={undefined}
                            toMonth={today}
                        />
                    </div>
                </PopoverContent>
            </Popover>
        </FormItem>
    );
};
