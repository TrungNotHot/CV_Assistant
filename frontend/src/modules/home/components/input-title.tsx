import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import { Search, X } from "lucide-react";

type Props = {
    value: string;
    onChange: (newValue: string) => void;
};

export const InputTitle = ({ value, onChange }: Props) => {
    return (
        <div className="relative">
            <Search className="text-muted-foreground absolute top-1/2 left-3 size-5 -translate-y-1/2 transform" />
            <Input
                type="text"
                placeholder="Search job titles..."
                className="px-10 py-2"
                value={value}
                onChange={(e) => onChange(e.target.value)}
            />
            <X
                className={cn(
                    "text-muted-foreground absolute top-1/2 right-3 size-4 -translate-y-1/2 transform",
                    value.length === 0 && "hidden",
                )}
                onClick={() => onChange("")}
            />
        </div>
    );
};
