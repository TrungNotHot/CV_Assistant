"use client";
import { Button } from "@/components/ui/button";
import { RefreshCw, ServerCrash } from "lucide-react";
import { useRouter } from "next/navigation";

export const ErrorState = () => {
    const router = useRouter();

    return (
        <div className="flex flex-col items-center justify-center rounded-lg bg-white p-6 text-center shadow-sm">
            <div className="rounded-full bg-gray-50 p-4">
                <ServerCrash className="text-foreground size-12" />
            </div>

            <div className="space-y-2">
                <h3 className="text-lg font-semibold text-gray-900">
                    Server Error
                </h3>
                <p className="mx-auto max-w-md text-gray-500">
                    Our server encountered an issue while processing your
                    request. We&apos;re working on fixing it.
                </p>

                <Button onClick={() => router.replace("/")} className="mt-4">
                    <RefreshCw />
                    Try again
                </Button>
            </div>
        </div>
    );
};
