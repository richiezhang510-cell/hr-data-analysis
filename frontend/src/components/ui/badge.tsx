import { cva, type VariantProps } from "class-variance-authority"

import { cn } from "@/lib/utils"

const badgeVariants = cva(
  "inline-flex items-center rounded-full border px-3 py-1 text-xs font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-neon-cyan/50 focus:ring-offset-2",
  {
    variants: {
      variant: {
        default: "border-neon-cyan/30 bg-neon-cyan/10 text-neon-cyan",
        secondary: "border-slate-700 bg-slate-800/80 text-neon-cyan",
        outline: "border-[rgba(56,189,248,0.2)] text-slate-300",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
)

export interface BadgeProps extends React.HTMLAttributes<HTMLDivElement>, VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />
}

export { Badge, badgeVariants }
