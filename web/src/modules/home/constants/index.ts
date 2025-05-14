type TimePreset = {
    value: string;
    label: string;
};

export const TIME_PRESETS: TimePreset[] = [
    { value: "0", label: "Today" },
    { value: "3", label: "3 months ago" },
    { value: "6", label: "6 months ago" },
    { value: "12", label: "1 year ago" },
    { value: "24", label: "2 years ago" },
];

export const POPULAR_SEARCHES: string[] = [
    "Frontend Developer",
    "Backend Developer",
    "Data Scientist",
    "UX Designer",
    "DevOps Engineer",
];

export const SKILL_BAR_SKELETON_WIDTHS = [80, 60, 50, 50, 40, 40];
