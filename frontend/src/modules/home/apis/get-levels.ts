export const getLevels = async () => {
    const res = await fetch("/api/levels");
    if (!res.ok) throw new Error("Failed to fetch level");
    return res.json();
};
