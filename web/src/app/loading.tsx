import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Loader2 } from "lucide-react";

export default function Loading() {
    return (
        <div className="container mx-auto max-w-5xl space-y-6 px-4 py-8">
            <div className="mb-8 flex justify-center">
                <div className="flex items-center gap-3">
                    <Loader2 className="text-primary h-6 w-6 animate-spin" />
                    <h1 className="text-2xl font-bold">
                        Loading Job Skills Explorer...
                    </h1>
                </div>
            </div>

            <Card>
                <CardContent className="space-y-6 p-6">
                    <div className="space-y-4">
                        <div className="flex flex-col gap-3 md:flex-row">
                            <Skeleton className="h-10 flex-1" />
                            <Skeleton className="h-10 w-24" />
                        </div>

                        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                            <div className="space-y-2">
                                <Skeleton className="h-4 w-32" />
                                <Skeleton className="h-10 w-full" />
                            </div>

                            <div className="space-y-2">
                                <Skeleton className="h-4 w-32" />
                                <Skeleton className="h-10 w-full" />
                            </div>
                        </div>
                    </div>
                </CardContent>
            </Card>

            <Card>
                <CardHeader>
                    <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
                        <div className="space-y-2">
                            <Skeleton className="h-6 w-48" />
                            <Skeleton className="h-4 w-64" />
                        </div>
                        <Skeleton className="h-9 w-64" />
                    </div>
                </CardHeader>

                <CardContent>
                    <div className="space-y-6">
                        <Skeleton className="h-10 w-full md:w-64" />

                        {Array(6)
                            .fill(0)
                            .map((_, index) => (
                                <div key={index} className="space-y-2">
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-2">
                                            <Skeleton className="h-5 w-32" />
                                            <Skeleton className="h-5 w-24" />
                                        </div>
                                        <Skeleton className="h-5 w-12" />
                                    </div>
                                    <Skeleton className="h-2.5 w-full" />
                                </div>
                            ))}

                        <div className="flex justify-center pt-6">
                            <Skeleton className="h-10 w-full md:w-40" />
                        </div>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
