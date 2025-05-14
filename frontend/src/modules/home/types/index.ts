type Skill = {
    skillName: string;
    skillType: "tech" | "soft";
    frequency: string;
};

export type Cursor = {
    lastFreq: number;
    lastName: string;
} | null;

type SkillApiResponse = {
    data: Skill[];
    nextCursor: Cursor;
    meta: {
        count: number;
        matched_job_count: number;
        duration_ms: number;
        timestamp: string;
    };
};
