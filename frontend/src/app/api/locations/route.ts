import { sql } from "@/db/pg";

export const GET = async () => {
    try {
        const results = await sql`
            SELECT DISTINCT location FROM jobs ORDER BY location;
        `;

        const locations = results.map((row) => row.location);
        return Response.json(locations);
    } catch (error) {
        console.error("Error fetching locations:", error);
        return Response.json(
            { error: "Failed to fetch locations" },
            { status: 500 },
        );
    }
};
