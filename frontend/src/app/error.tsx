"use client";

import { Button } from "@/components/ui/button";
import {
    Card,
    CardContent,
    CardFooter,
    CardHeader,
    CardTitle,
} from "@/components/ui/card";
import { AlertCircle, RefreshCw } from "lucide-react";
import { useEffect } from "react";

export default function Error({
    error,
    reset,
}: {
    error: Error & { digest?: string };
    reset: () => void;
}) {
    useEffect(() => {
        console.error(error);
    }, [error]);

    return (
        <div className="container mx-auto flex min-h-[60vh] items-center justify-center px-4 py-16">
            <Card className="w-full max-w-md">
                <CardHeader>
                    <div className="flex items-center gap-2 text-red-500">
                        <AlertCircle className="h-6 w-6" />
                        <CardTitle>Something went wrong</CardTitle>
                    </div>
                </CardHeader>
                <CardContent>
                    <p className="text-gray-500">
                        We encountered an error while loading the Job Skills
                        Explorer. This could be due to a network issue or a
                        temporary server problem.
                    </p>
                    <div className="mt-4 rounded-md border border-red-100 bg-red-50 p-3 text-sm text-red-800">
                        {error.message || "An unexpected error occurred"}
                    </div>
                </CardContent>
                <CardFooter className="flex justify-end gap-3">
                    <Button
                        variant="outline"
                        onClick={() => (window.location.href = "/")}
                    >
                        Go Home
                    </Button>
                    <Button onClick={reset} className="flex items-center gap-2">
                        <RefreshCw className="h-4 w-4" />
                        Try Again
                    </Button>
                </CardFooter>
            </Card>
        </div>
    );
}
