function normalizeText(text: string): string {
    return text
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
}

export function isMatch(a: string, b: string): boolean {
    const normA = normalizeText(a);
    const normB = normalizeText(b);
    return normA.includes(normB) || normB.includes(normA);
}

export function getMonthAgo(months: number): Date {
    const date = new Date();
    date.setMonth(date.getMonth() - months);
    return date;
}
