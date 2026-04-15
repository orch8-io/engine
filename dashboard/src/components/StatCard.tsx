interface Props {
  label: string;
  value: number | string;
  color?: string;
}

export default function StatCard({ label, value, color = "text-foreground" }: Props) {
  return (
    <div className="rounded-lg border border-card-border bg-card p-5">
      <div className="text-sm text-muted mb-1">{label}</div>
      <div className={`text-3xl font-bold ${color}`}>{value}</div>
    </div>
  );
}
