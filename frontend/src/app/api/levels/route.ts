import { sql } from "@/db/pg";

export const GET = async () => {
    try {
        const results = await sql`
             SELECT DISTINCT seniority_level FROM jobs ORDER BY seniority_level desc; 
        `;

        const levelsTemp = results.map((row) => row.seniorityLevel);
        // console.log("🚀 ~ GET ~ levelsTemp:", levelsTemp);

        const levels: string[] = [
            "Intern",
            "Junior",
            "Mid-level",
            "Senior",
            "Lead",
            "Manager",
            "Director",
            "VP",
            "Principal",
            "CTO",
        ];
        return Response.json(levels);
    } catch (error) {
        console.error("Error fetching levels:", error);
        return Response.json(
            { error: "Failed to fetch levels" },
            { status: 500 },
        );
    }
};
