export const getLocations = async () => {
    const res = await fetch("/api/locations");
    if (!res.ok) throw new Error("Failed to fetch location");
    return res.json();
};
