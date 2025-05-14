import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useFilters } from "@/hooks/use-filters";
import { Loader2 } from "lucide-react";
import { SKILL_BAR_SKELETON_WIDTHS } from "../constants";
import { useGetSkillsInfinite } from "../hooks/use-skill";
import { EmptyResults } from "./empty-results";
import { ErrorState } from "./error-state";
import { MetaDataDisplay } from "./meta-data-display";
import { SkillBar } from "./skill-bar";

export const SkillResults = () => {
    const {
        data,
        fetchNextPage,
        hasNextPage,
        isFetchingNextPage,
        isLoading,
        isError,
    } = useGetSkillsInfinite();

    const [filters] = useFilters();

    if (isLoading) {
        return <SkillResultsSkeleton />;
    }

    if (isError) {
        return <ErrorState />;
    }

    const allSkills = data?.pages.flatMap((page) => page.data) ?? [];
    const technicalSkills = allSkills.filter((s) => s.skillType === "tech");
    const softSkills = allSkills.filter((s) => s.skillType === "soft");

    if (allSkills.length === 0) {
        return <EmptyResults />;
    }

    const meta = data?.pages[0].meta;

    return (
        <div className="rounded-lg bg-white p-6 shadow-sm">
            <MetaDataDisplay {...meta} count={allSkills.length} />
            <Tabs defaultValue="all">
                <div className="mb-6 flex items-center justify-between">
                    <h2 className="text-2xl font-bold">
                        {filters.jobTitle} Skills
                    </h2>

                    <TabsList defaultValue="all">
                        <TabsTrigger value="all">All Skills</TabsTrigger>
                        <TabsTrigger value="tech">Technical</TabsTrigger>
                        <TabsTrigger value="soft">Soft Skills</TabsTrigger>
                    </TabsList>
                </div>

                <TabsContent className="space-y-6" value="all">
                    {allSkills.map((skill, index) => (
                        <SkillBar
                            key={index}
                            name={skill.skillName}
                            frequency={skill.frequency}
                            type={skill.skillType}
                        />
                    ))}
                </TabsContent>

                <TabsContent className="space-y-6" value="tech">
                    {technicalSkills.map((skill, index) => (
                        <SkillBar
                            key={index}
                            name={skill.skillName}
                            frequency={skill.frequency}
                            type={skill.skillType}
                        />
                    ))}
                </TabsContent>

                <TabsContent className="space-y-6" value="soft">
                    {softSkills.map((skill, index) => (
                        <SkillBar
                            key={index}
                            name={skill.skillName}
                            frequency={skill.frequency}
                            type={skill.skillType}
                        />
                    ))}
                </TabsContent>

                {hasNextPage && (
                    <div className="flex justify-center pt-6">
                        <Button
                            onClick={() => fetchNextPage()}
                            disabled={isFetchingNextPage}
                            variant="secondary"
                            className="w-full md:w-auto"
                        >
                            {isFetchingNextPage ? (
                                <span className="flex items-center gap-2">
                                    <Loader2 className="h-4 w-4 animate-spin" />
                                    Loading more skills...
                                </span>
                            ) : (
                                "Load More Skills"
                            )}
                        </Button>
                    </div>
                )}
            </Tabs>
        </div>
    );
};

export const SkillResultsSkeleton = () => {
    return (
        <div className="rounded-lg bg-white p-6 shadow-sm">
            <div className="space-y-6">
                <div className="flex flex-col justify-between gap-4 sm:flex-row">
                    <Skeleton className="h-10 w-full sm:w-64" />
                    <Skeleton className="h-10 w-full sm:w-48" />
                </div>

                {SKILL_BAR_SKELETON_WIDTHS.map((_, index) => (
                    <div key={index} className="space-y-2">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                                <Skeleton className="h-6 w-32" />
                                <Skeleton className="h-6 w-24" />
                            </div>
                            <Skeleton className="h-6 w-12" />
                        </div>
                        <Skeleton
                            className="h-4"
                            style={{
                                width: `${SKILL_BAR_SKELETON_WIDTHS[index]}%`,
                            }}
                        />
                    </div>
                ))}
            </div>
        </div>
    );
};
