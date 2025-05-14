import { format } from "date-fns";
import { BarChart3, Calendar, Clock, Database } from "lucide-react";

interface MetaDataProps {
    count: number;
    matchedJobCount: number;
    duration: number;
    timestamp: string;
}

export function MetaDataDisplay({
    count,
    matchedJobCount,
    duration,
    timestamp,
}: MetaDataProps) {
    const formattedDate = format(timestamp, "dd/MM/yyyy");
    const formattedTime = format(timestamp, "HH:mm");

    const formattedDuration =
        duration < 1000 ? `${duration}ms` : `${(duration / 1000).toFixed(2)}s`;

    return (
        <div className="mb-6 border-b border-gray-200 pb-6">
            <div className="flex flex-wrap justify-between gap-3">
                <div className="flex items-center gap-2 text-sm">
                    <div className="rounded-full bg-blue-100 p-1.5">
                        <BarChart3 className="h-4 w-4 text-blue-600" />
                    </div>
                    <span className="text-gray-500">Results:</span>
                    <span className="font-medium">{count} skills</span>
                </div>
                <div className="flex items-center gap-2 text-sm">
                    <div className="rounded-full bg-green-100 p-1.5">
                        <Database className="h-4 w-4 text-green-600" />
                    </div>
                    <span className="text-gray-500">Matched:</span>
                    <span className="font-medium">
                        {matchedJobCount.toLocaleString()} jobs
                    </span>
                </div>
                <div className="flex items-center gap-2 text-sm">
                    <div className="rounded-full bg-amber-100 p-1.5">
                        <Clock className="h-4 w-4 text-amber-600" />
                    </div>
                    <span className="text-gray-500">Query time:</span>
                    <span className="font-medium">{formattedDuration}</span>
                </div>
                <div className="flex items-center gap-2 text-sm">
                    <div className="rounded-full bg-purple-100 p-1.5">
                        <Calendar className="h-4 w-4 text-purple-600" />
                    </div>
                    <span className="text-gray-500">Updated:</span>
                    <span className="font-medium">{formattedDate}</span>
                </div>
            </div>
        </div>
    );
}
